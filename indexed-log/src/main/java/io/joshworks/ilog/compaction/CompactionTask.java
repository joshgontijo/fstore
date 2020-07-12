package io.joshworks.ilog.compaction;

import io.joshworks.ilog.Segment;
import io.joshworks.ilog.View;
import io.joshworks.ilog.compaction.combiner.SegmentCombiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

public class CompactionTask<T extends Segment> implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(CompactionTask.class);

    private final int level;
    private final SegmentCombiner combiner;
    private final List<T> segments;
    private final Consumer<CompactionResult<T>> onComplete;
    private final View<T> view;

    public CompactionTask(CompactionEvent<T> event) {
        this.level = event.level;
        this.combiner = event.combiner;
        this.segments = new ArrayList<>(event.segments);
        this.onComplete = event.onComplete;
        this.view = event.view;
    }

    @Override
    public void run() {
        T output = null;
        try {
            long newSegmentLogSize = segments.stream().mapToLong(T::size).sum();
            long estimatedEntries = segments.stream().mapToLong(T::entries).sum();

            String names = Arrays.toString(segments.stream().map(T::name).toArray());
            logger.info("Compacting {} from level {} using {}, new segment computed size: {}, estimated entry count: {}",
                    names,
                    level,
                    combiner.getClass().getSimpleName(),
                    newSegmentLogSize,
                    estimatedEntries);

            for (int i = 0; i < segments.size(); i++) {
                T segment = segments.get(i);
                logger.info("Segment[{}] {}", i, segment);
            }

            long start = System.currentTimeMillis();

            output = view.newSegment(level + 1, estimatedEntries);

            combiner.merge(segments, output);
            output.flush();

            logger.info("Compaction completed, took {}ms", (System.currentTimeMillis() - start));
            logger.info("Result segment {}", output);

            onComplete.accept(CompactionResult.success(segments, output, level));

        } catch (Exception e) {
            logger.error("Failed to compact", e);
            onComplete.accept(CompactionResult.failure(segments, output, level, e));
        }
    }
}
