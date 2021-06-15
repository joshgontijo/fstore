package io.joshworks.ilog.compaction;

import io.joshworks.ilog.Segment;
import io.joshworks.ilog.View;
import io.joshworks.ilog.compaction.combiner.SegmentCombiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CompactionTask  {

    private static final Logger logger = LoggerFactory.getLogger(CompactionTask.class);

    private final int level;
    private final SegmentCombiner combiner;
    private final List<Segment> segments;
    private final View view;

    public CompactionTask(View view, List<Segment> segments, SegmentCombiner combiner, int level) {
        this.level = level;
        this.combiner = combiner;
        this.segments = new ArrayList<>(segments);
        this.view = view;
    }

    CompactionResult compact() {
        if (CompactionRunner.closed.get()) {
            return CompactionResult.aborted(view, segments, level);
        }
        Segment output = null;
        try {
            long newSegmentLogSize = segments.stream().mapToLong(Segment::size).sum();
            long estimatedEntries = segments.stream().mapToLong(Segment::entries).sum();

            String names = Arrays.toString(segments.stream().map(Segment::name).toArray());
            logger.info("Compacting {} from level {} using {}, new segment computed size: {}, estimated entry count: {}",
                    names,
                    level,
                    combiner.getClass().getSimpleName(),
                    newSegmentLogSize,
                    estimatedEntries);

            for (int i = 0; i < segments.size(); i++) {
                Segment segment = segments.get(i);
                logger.info("Segment[{}] {}", i, segment);
            }

            long start = System.currentTimeMillis();

            //no max size for output segments
            output = view.newSegment(level + 1, Segment.NO_MAX_SIZE, estimatedEntries);
            logger.info("Output segment {}", output);

            combiner.merge(segments, output);
            output.flush();

            logger.info("Compaction completed, took {}ms", (System.currentTimeMillis() - start));


            return CompactionResult.success(view, segments, output, level);

        } catch (Exception e) {
            logger.error("Failed to compact", e);
            return CompactionResult.failure(view, segments, output, level, e);
        }
    }
}
