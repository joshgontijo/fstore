package io.joshworks.fstore.log.appender.compaction;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.metrics.MonitoredThreadPool;
import io.joshworks.fstore.core.util.Logging;
import io.joshworks.fstore.core.util.Threads;
import io.joshworks.fstore.log.appender.compaction.combiner.SegmentCombiner;
import io.joshworks.fstore.log.appender.level.Levels;
import io.joshworks.fstore.log.appender.naming.NamingStrategy;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.SegmentFactory;
import io.joshworks.fstore.log.utils.LogFileUtils;
import org.slf4j.Logger;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class DefaultCompactor<T> implements ICompactor {

    private final Logger logger;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final File directory;
    private final SegmentCombiner<T> segmentCombiner;
    private final SegmentFactory<T> segmentFactory;
    private final StorageMode storageMode;
    private final Serializer<T> serializer;
    private final BufferPool bufferPool;
    private final NamingStrategy namingStrategy;
    private final int compactionThreshold;
    private final String name;
    private final Levels<T> levels;
    private final boolean threadPerLevel;
    private final int readPageSize;
    private final double checksumProbability;
    private final Set<Log<T>> compacting = new CopyOnWriteArraySet<>();

    private final Map<String, ExecutorService> levelCompaction = new ConcurrentHashMap<>();

    private final ExecutorService cleanupWorker;
    private final ExecutorService coordinator;

    public DefaultCompactor(File directory,
                            SegmentCombiner<T> segmentCombiner,
                            SegmentFactory<T> segmentFactory,
                            StorageMode storageMode,
                            Serializer<T> serializer,
                            BufferPool bufferPool,
                            NamingStrategy namingStrategy,
                            int compactionThreshold,
                            String name,
                            Levels<T> levels,
                            boolean threadPerLevel,
                            int readPageSize,
                            double checksumProbability) {
        this.directory = directory;
        this.segmentCombiner = segmentCombiner;
        this.segmentFactory = segmentFactory;
        this.storageMode = storageMode;
        this.serializer = serializer;
        this.bufferPool = bufferPool;
        this.namingStrategy = namingStrategy;
        this.compactionThreshold = compactionThreshold;
        this.name = name;
        this.levels = levels;
        this.threadPerLevel = threadPerLevel;
        this.logger = Logging.namedLogger(name, "compactor");
        this.readPageSize = readPageSize;
        this.checksumProbability = checksumProbability;
        this.cleanupWorker = singleThreadExecutor(name + "-compaction-cleanup");
        this.coordinator = singleThreadExecutor(name + "-compaction-coordinator");
    }

    private static ExecutorService levelExecutor(String name) {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(0, 1, 1,
                TimeUnit.MINUTES,
                new ArrayBlockingQueue<>(5),
                Threads.namedThreadFactory(name),
                new ThreadPoolExecutor.DiscardPolicy());
        return new MonitoredThreadPool(name, executor);
    }

    private static ExecutorService singleThreadExecutor(String name) {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(0, 1, 1,
                TimeUnit.MINUTES,
                new LinkedBlockingDeque<>(),
                Threads.namedThreadFactory(name),
                new ThreadPoolExecutor.DiscardPolicy());
        return new MonitoredThreadPool(name, executor);
    }

    @Override
    public void compact(boolean force) {
        scheduleCompaction(1, force);
    }

    private void scheduleCompaction(int level, boolean force) {
        if (closed.get()) {
            logger.info("Close requested, ignoring compaction request");
            return;
        }
        coordinator.execute(() -> processCompaction(level, force));
    }

    private void processCompaction(int level, boolean force) {
        if (closed.get()) {
            logger.info("Close requested, ignoring compaction");
            return;
        }
        List<Log<T>> segmentsForCompaction = segmentsForCompaction(level, force);
        if (segmentsForCompaction.isEmpty()) {
            return;
        }

        logger.info("Compacting level {}", level);

        File targetFile = LogFileUtils.newSegmentFile(directory, namingStrategy, level + 1);
        var event = new CompactionEvent<>(
                segmentsForCompaction,
                segmentCombiner,
                targetFile,
                segmentFactory,
                storageMode,
                serializer,
                bufferPool,
                name,
                level,
                readPageSize,
                checksumProbability,
                this::cleanup);

        submitCompaction(event);

    }

    private void submitCompaction(CompactionEvent<T> event) {
        executorFor(event.level).submit(() -> {
            if (!closed.get()) {
                new CompactionTask<>(event).run();
            }
        });
    }

    private String executorName(int level) {
        return threadPerLevel ? name + "-compaction-level-" + level : name + "-compaction";
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

            levels.lock(() -> {
                if (target.entries() == 0) {
                    logger.info("No entries were found in the result segment {}, deleting", target.name());
                    deleteAll(List.of(target));
                    levels.remove(sources);
                } else {
                    levels.merge(sources, target);
                }

                compacting.removeAll(sources);

                //force does not propagate to upper level
                scheduleCompaction(level, false);
                scheduleCompaction(level + 1, false);
                deleteAll(sources);
            });
        });
    }

    //returns either the segments to be compacted or empty if not enough segments
    private List<Log<T>> segmentsForCompaction(int level, boolean force) {
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
            if (force) {
                if (toBeCompacted.size() <= 1) {
                    //no segment regardless
                    return List.of();
                }
            } else if (toBeCompacted.size() < compactionThreshold) {
                return List.of();
            }

            compacting.addAll(toBeCompacted);
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
        return levelCompaction.compute(executorName, (k, v) -> v == null ? levelExecutor(executorName) : v);
    }

    @Override
    public synchronized void close() {
        if (closed.compareAndSet(false, true)) {
            logger.info("Closing compactor");
            //Order matters here
            coordinator.shutdown();
            levelCompaction.values().forEach(exec -> Threads.awaitTerminationOf(exec, 2, TimeUnit.SECONDS, () -> logger.info("Awaiting active compaction task")));
            Threads.awaitTerminationOf(cleanupWorker, 2, TimeUnit.SECONDS, () -> logger.info("Awaiting active compaction cleanup task"));
        }
    }
}
