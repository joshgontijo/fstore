package io.joshworks.ilog.compaction;

import io.joshworks.fstore.core.metrics.MonitoredThreadPool;
import io.joshworks.fstore.core.util.Threads;
import io.joshworks.ilog.index.Index;
import io.joshworks.ilog.IndexedSegment;
import io.joshworks.ilog.View;
import io.joshworks.ilog.compaction.combiner.SegmentCombiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

public class Compactor<T extends IndexedSegment> {

    private static final Logger logger = LoggerFactory.getLogger(Compactor.class);
    private final AtomicBoolean closed = new AtomicBoolean();
    private final View<T> view;
    private final String name;
    private final SegmentCombiner segmentCombiner;
    private final boolean threadPerLevel;
    private final int compactionThreshold;
    private final Set<T> compacting = new CopyOnWriteArraySet<>();

    private final Map<String, ExecutorService> levelCompaction = new ConcurrentHashMap<>();

    private final ExecutorService cleanupWorker;
    private final ExecutorService coordinator;

    public Compactor(View<T> view,
                     String name,
                     SegmentCombiner segmentCombiner,
                     boolean threadPerLevel,
                     int compactionThreshold) {
        this.view = view;
        this.name = name;
        this.segmentCombiner = segmentCombiner;
        this.threadPerLevel = threadPerLevel;
        this.compactionThreshold = compactionThreshold;
        this.cleanupWorker = singleThreadExecutor("compaction-cleanup");
        this.coordinator = singleThreadExecutor("-compaction-coordinator");
    }

    public void compact(boolean force) {
        scheduleCompaction(0, force);
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
        List<T> segmentsForCompaction = segmentsForCompaction(level, force);
        if (segmentsForCompaction.isEmpty()) {
            return;
        }

        logger.info("Compacting level {}", level);

        var event = new CompactionEvent<>(view, segmentsForCompaction, segmentCombiner, level, this::cleanup);
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
            T target = result.target;
            int level = result.level;
            List<T> sources = Collections.unmodifiableList(result.sources);

            if (!result.successful()) {
                //TODO
                logger.error("Compaction error", result.exception);
                logger.info("Deleting failed merge result segment");
                target.delete();
                return;
            }

            view.lock(() -> {
                if (target.entries() == 0) {
                    logger.info("No entries were found in the result segment {}, deleting", target.name());
                    deleteAll(List.of(target));
                    view.remove(sources);
                } else {
                    view.merge(sources, target);
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
    private List<T> segmentsForCompaction(int level, boolean force) {
        return view.apply(level, segments -> {
            List<T> toBeCompacted = new ArrayList<>();
            for (T segment : segments) {
                if (!compacting.contains(segment) && segment.readOnly()) {
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
            long indexSize = toBeCompacted.stream().mapToLong(T::indexSize).sum();
            if (indexSize > Index.MAX_SIZE) {
                logger.info("New index size will be greater than {}, not compacting", Index.MAX_SIZE);
                return List.of();
            }

            compacting.addAll(toBeCompacted);
            return toBeCompacted;
        });
    }

    //delete all source segments only if all of them are not being used
    private void deleteAll(List<T> segments) {
        for (T segment : segments) {
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


    public synchronized void close() {
        if (closed.compareAndSet(false, true)) {
            logger.info("Closing compactor");
            //Order matters here
            coordinator.shutdown();
            levelCompaction.values().forEach(exec -> Threads.awaitTerminationOf(exec, 2, TimeUnit.SECONDS, () -> logger.info("Awaiting active compaction task")));
            Threads.awaitTerminationOf(cleanupWorker, 2, TimeUnit.SECONDS, () -> logger.info("Awaiting active compaction cleanup task"));
        }
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
}
