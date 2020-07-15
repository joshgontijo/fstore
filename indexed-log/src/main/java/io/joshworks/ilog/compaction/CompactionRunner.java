package io.joshworks.ilog.compaction;

import io.joshworks.fstore.core.metrics.MonitoredThreadPool;
import io.joshworks.fstore.core.util.Threads;
import io.joshworks.ilog.Segment;
import io.joshworks.ilog.View;
import io.joshworks.ilog.compaction.combiner.SegmentCombiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class CompactionRunner {

    private static final Logger logger = LoggerFactory.getLogger(CompactionRunner.class);
    static final AtomicBoolean closed = new AtomicBoolean();

    //TODO - MOVE TO VIEW OR USE A MAP


    //global thread pools
    private static final ExecutorService cleanupWorker = threadPool("compaction-cleanup", 1);
    private static final ExecutorService coordinator = threadPool("compaction-coordinator", 1);
    private static final ExecutorService compactionWorker = threadPool("compaction", 5); // TODO make configurable

    private final int threshold;
    private final boolean autoCompaction;
    private final SegmentCombiner combiner;

    public CompactionRunner(int threshold, SegmentCombiner combiner) {
        this.threshold = threshold;
        this.combiner = combiner;
        this.autoCompaction = threshold > 0;
        if (threshold < 0) {
            logger.info("Compaction is disabled");
        }
    }

    public void compact(View view, boolean force) {
        if (!autoCompaction && !force) {
            return;
        }
        scheduleCompaction(view, 0, force);
    }

    private void scheduleCompaction(View view, int level, boolean force) {
        if (closed.get()) {
            logger.info("Close requested, ignoring compaction request");
            return;
        }
        coordinator.execute(() -> processCompaction(view, level, force));
    }

    private void processCompaction(View view, int level, boolean force) {
        if (closed.get()) {
            logger.info("Close requested, ignoring compaction");
            return;
        }
        List<Segment> segmentsForCompaction = segmentsForCompaction(view, level, force);
        if (segmentsForCompaction.isEmpty() || segmentsForCompaction.size() < threshold) {
            return;
        }

        logger.info("Compacting level {}", level);

        var event = new CompactionTask(view, segmentsForCompaction, combiner, level);

        CompletableFuture.supplyAsync(event::compact, compactionWorker)
                .thenAcceptAsync(this::cleanup, cleanupWorker);

    }

    private void cleanup(CompactionResult result) {
        View view = result.view;
        Segment target = result.target;
        int level = result.level;
        List<Segment> sources = Collections.unmodifiableList(result.sources);

        if (CompactionResult.State.FAILED.equals(result.state())) {
            //TODO
            logger.error("Compaction error", result.exception);
            logger.info("Deleting failed merge result segment");
            target.delete();
            return;
        }

        if (CompactionResult.State.ABORTED.equals(result.state())) {
            logger.info("Compaction aborted for segments: {}", sources);
            view.removeFromCompacting(sources);
            return;
        }

        //COMPLETED
        view.lock(() -> {
            if (target.entries() == 0) {
                logger.info("No entries were found in the result segment {}, deleting", target.name());
                target.delete(); //safe to delete as it's not used yet
                view.remove(sources);
            } else {
                view.merge(sources, target);
            }

            view.removeFromCompacting(sources);

            //force does not propagate to upper level
            scheduleCompaction(view, level, false);
            scheduleCompaction(view, level + 1, false);
        });
    }

    //returns either the segments to be compacted or empty if not enough segments
    private List<Segment> segmentsForCompaction(View view, int level, boolean force) {
        return view.apply(level, segments -> {
            List<Segment> toBeCompacted = new ArrayList<>();
            for (Segment segment : segments) {
                if (!view.isCompacting(segment) && segment.readOnly()) {
                    toBeCompacted.add(segment);
                }
                if (toBeCompacted.size() >= threshold) {
                    break;
                }
            }
            if (force) {
                if (toBeCompacted.size() <= 1) {
                    //no segment regardless
                    return List.of();
                }
            } else if (toBeCompacted.size() < threshold) {
                return List.of();
            }

            view.addToCompacting(toBeCompacted);
            return toBeCompacted;
        });
    }


    public static synchronized void shutdown() {
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
