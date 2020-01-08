package io.joshworks.ilog.compaction;

import io.joshworks.ilog.IndexedSegment;
import io.joshworks.ilog.View;
import io.joshworks.ilog.compaction.combiner.SegmentCombiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

public class CompactionTask implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(CompactionTask.class);

    private final int level;
    private final SegmentCombiner combiner;
    private final List<IndexedSegment> segments;
    private final Consumer<CompactionResult> onComplete;
    private final View view;

    public CompactionTask(CompactionEvent event) {
        this.level = event.level;
        this.combiner = event.combiner;
        this.segments = new ArrayList<>(event.segments);
        this.onComplete = event.onComplete;
        this.view = event.view;
    }

    @Override
    public void run() {
        IndexedSegment output = null;
        try {
            long newSegmentLogSize = segments.stream().mapToLong(IndexedSegment::size).sum();
            long estimatedEntries = segments.stream().mapToLong(IndexedSegment::entries).sum();
            long estimatedIndexSize = segments.stream().mapToLong(IndexedSegment::indexSize).sum();

            String names = Arrays.toString(segments.stream().map(IndexedSegment::name).toArray());
            logger.info("Compacting {} from level {} using {}, new segment computed size: {}, estimated entry count: {}, estimated index size: {}",
                    names,
                    level,
                    combiner.getClass().getSimpleName(),
                    newSegmentLogSize,
                    estimatedEntries,
                    estimatedIndexSize);

            for (int i = 0; i < segments.size(); i++) {
                IndexedSegment segment = segments.get(i);
                logger.info("Segment[{}] {}", i, segment);
            }

            long start = System.currentTimeMillis();

            output = view.newSegment(level + 1, estimatedIndexSize);

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
