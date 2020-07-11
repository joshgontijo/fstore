package io.joshworks.ilog.compaction.combiner;

import io.joshworks.ilog.IndexedSegment;

import java.util.List;

public class ConcatenateCombiner implements SegmentCombiner {

    @Override
    public void merge(List<? extends IndexedSegment> segments, IndexedSegment output) {
        segments.forEach(c -> c.transferTo(output));
        output.reindex();
    }

}
