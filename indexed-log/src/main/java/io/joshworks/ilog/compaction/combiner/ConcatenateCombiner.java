package io.joshworks.ilog.compaction.combiner;

import io.joshworks.ilog.Segment;

import java.util.List;

public class ConcatenateCombiner implements SegmentCombiner {

    @Override
    public void merge(List<? extends Segment> segments, Segment output) {
        segments.forEach(c -> c.transferTo(output));
        output.restore();
    }

}
