package io.joshworks.fstore.log.appender.compaction;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.util.Logging;
import io.joshworks.fstore.core.util.Threads;
import io.joshworks.fstore.log.appender.compaction.combiner.SegmentCombiner;
import io.joshworks.fstore.log.appender.level.Levels;
import io.joshworks.fstore.log.appender.naming.NamingStrategy;
import io.joshworks.fstore.log.record.IDataStream;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.SegmentFactory;
import io.joshworks.fstore.log.utils.LogFileUtils;
import org.slf4j.Logger;

import java.io.Closeable;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class Compactor<T> implements Closeable {

    private final Logger logger;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final File directory;
    private final SegmentCombiner<T> segmentCombiner;
    private final SegmentFactory<T> segmentFactory;
    private final StorageMode storageMode;
    private final Serializer<T> serializer;
    private final IDataStream dataStream;
    private final NamingStrategy namingStrategy;
    private final int compactionThreshold;
    private final String magic;
    private final String name;
    private final Levels<T> levels;
    private final boolean threadPerLevel;
    private final Set<Log<T>> compacting = new CopyOnWriteArraySet<>();

    private final Map<String, ExecutorService> levelCompaction = new ConcurrentHashMap<>();
    private final ExecutorService cleanupWorker = Executors.newSingleThreadExecutor(Threads.namedThreadFactory("compaction-cleanup"));
    private final ExecutorService coordinator = Executors.newSingleThreadExecutor(Threads.namedThreadFactory("compaction-coordinator"));

    public Compactor(File directory,
                     SegmentCombiner<T> segmentCombiner,
                     SegmentFactory<T> segmentFactory,
                     StorageMode storageMode,
                     Serializer<T> serializer,
                     IDataStream dataStream,
                     NamingStrategy namingStrategy,
                     int compactionThreshold,
                     String magic,
                     String name,
                     Levels<T> levels,
                     boolean threadPerLevel) {
        this.directory = directory;
        this.segmentCombiner = segmentCombiner;
        this.segmentFactory = segmentFactory;
        this.storageMode = storageMode;
        this.serializer = serializer;
        this.dataStream = dataStream;
        this.namingStrategy = namingStrategy;
        this.compactionThreshold = compactionThreshold;
        this.magic = magic;
        this.name = name;
        this.levels = levels;
        this.threadPerLevel = threadPerLevel;
        this.logger = Logging.namedLogger(name, "compactor");
    }

    public void compact() {
        scheduleCompaction(1);
    }

    private void scheduleCompaction(int level) {
        if (closed.get()) {
            logger.info("Close requested, ignoring compaction request");
            return;
        }
        coordinator.execute(() -> processCompaction(level));
    }

    private void processCompaction(int level) {
        if (closed.get()) {
            logger.info("Close requested, ignoring compaction");
            return;
        }
        List<Log<T>> segmentsForCompaction = segmentsForCompaction(level);
        if (segmentsForCompaction.size() < compactionThreshold) {
            return;
        }
        compacting.addAll(segmentsForCompaction);

        logger.info("Compacting level {}", level);

        File targetFile = LogFileUtils.newSegmentFile(directory, namingStrategy, level + 1);
        var event = new CompactionEvent<>(
                segmentsForCompaction,
                segmentCombiner,
                targetFile,
                segmentFactory,
                storageMode,
                serializer,
                dataStream,
                name,
                level,
                magic,
                this::cleanup);

        submitCompaction(event);

    }

    private void submitCompaction(CompactionEvent<T> event) {
        executorFor(event.level).submit(() -> {
            if (closed.get()) {
                return;
            }
            new CompactionTask<>(event).run();
        });
    }

    private String executorName(int level) {
        return threadPerLevel ? "compaction-level-" + level : "compaction";
    }

    private void cleanup(CompactionResult<T> result) {
        cleanupWorker.execute(() -> {
            Log<T> target = result.target;
            int level = result.level;
            List<Log<T>> sources = Collections.unmodifiableList(result.sources);

            if (!result.successful()) {
                //TODO
                logger.error("Compaction error", result.exception);
                logger.info("Deleting failed merge result segment");
                target.delete();
                return;
            }

            if (target.entries() == 0) {
                logger.info("No entries were found in the result segment {}, deleting", target.name());
                deleteAll(List.of(target));
                levels.remove(sources);
            } else {
                levels.merge(sources, target);
            }

            compacting.removeAll(sources);

            scheduleCompaction(level);
            scheduleCompaction(level + 1);
            deleteAll(sources);
        });
    }

    private List<Log<T>> segmentsForCompaction(int level) {
        if (level <= 0) {
            throw new IllegalArgumentException("Level must be greater than zero");
        }
        return levels.apply(level, segments -> {
            List<Log<T>> toBeCompacted = new ArrayList<>();
            for (Log<T> segment : segments) {
                if (!compacting.contains(segment)) {
                    toBeCompacted.add(segment);
                }
                if (toBeCompacted.size() >= compactionThreshold) {
                    break;
                }
            }
            return toBeCompacted;
        });
    }

    //delete all source segments only if all of them are not being used
    private void deleteAll(List<Log<T>> segments) {
        for (Log<T> segment : segments) {
            String segmentName = segment.name();
            try {
                segment.delete();
            } catch (Exception e) {
                logger.error("Failed to delete " + segmentName, e);
            }
        }
    }

    private ExecutorService executorFor(int level) {
        String executorName = executorName(level);
        return levelCompaction.compute(executorName, (k, v) -> {
            if (v == null) {
                return Executors.newSingleThreadExecutor(Threads.namedThreadFactory(executorName));
            }
            return v;
        });
    }


    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            logger.info("Closing compactor");
            //Order matters here
            coordinator.shutdown();
            levelCompaction.values().forEach(exec -> Threads.awaitTerminationOf(exec, 2, TimeUnit.SECONDS, () -> logger.info("Awaiting active compaction task")));
            Threads.awaitTerminationOf(cleanupWorker, 2, TimeUnit.SECONDS, () -> logger.info("Awaiting active compaction cleanup task"));
        }
    }
}
