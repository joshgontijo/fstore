package io.joshworks.ilog.compaction;

import io.joshworks.fstore.core.metrics.MonitoredThreadPool;
import io.joshworks.fstore.core.util.Threads;
import io.joshworks.ilog.IndexedSegment;
import io.joshworks.ilog.View;
import io.joshworks.ilog.compaction.combiner.SegmentCombiner;
import io.joshworks.ilog.index.Index;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
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
    private final SegmentCombiner segmentCombiner;
    private final int compactionThreshold;
    private final Set<T> compacting = new CopyOnWriteArraySet<>();


    private final ExecutorService cleanupWorker;
    private final ExecutorService coordinator;
    private final ExecutorService compactionWorker;

    public Compactor(View<T> view,
                     SegmentCombiner segmentCombiner,
                     int compactionThreshold,
                     int compactionThreads) {
        this.view = view;
        this.segmentCombiner = segmentCombiner;
        this.compactionThreshold = compactionThreshold;
        this.compactionWorker = threadPool("compaction", compactionThreads);
        this.cleanupWorker = threadPool("compaction-cleanup", 1);
        this.coordinator = threadPool("compaction-coordinator", 1);

        if (compactionDisabled()) {
            logger.info("Compaction is disabled");
        }
    }

    private boolean compactionDisabled() {
        return this.compactionThreshold <= 0;
    }

    public void compact(boolean force) {
        if (compactionDisabled() && !force) {
            return;
        }
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
        if (segmentsForCompaction.isEmpty() || segmentsForCompaction.size() < compactionThreshold) {
            return;
        }

        logger.info("Compacting level {}", level);

        var event = new CompactionEvent<>(view, segmentsForCompaction, segmentCombiner, level, this::cleanup);
        compactionWorker.submit(() -> {
            if (!closed.get()) {
                new CompactionTask<>(event).run();
            }
        });

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


    public synchronized void close() {
        if (closed.compareAndSet(false, true)) {
            logger.info("Closing compactor");
            //Order matters here
            coordinator.shutdown();
            Threads.awaitTerminationOf(compactionWorker, 2, TimeUnit.SECONDS, () -> logger.info("Awaiting active compaction task"));
            Threads.awaitTerminationOf(cleanupWorker, 2, TimeUnit.SECONDS, () -> logger.info("Awaiting active compaction cleanup task"));
        }
    }

    private static ExecutorService threadPool(String name, int poolSize) {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(poolSize, poolSize, 1,
                TimeUnit.MINUTES,
                new LinkedBlockingDeque<>(),
                Threads.namePrefixedThreadFactory(name),
                new ThreadPoolExecutor.DiscardPolicy());
        return new MonitoredThreadPool(name, executor);
    }
}
