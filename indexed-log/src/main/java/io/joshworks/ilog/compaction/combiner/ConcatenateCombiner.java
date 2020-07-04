package io.joshworks.ilog.compaction.combiner;

import io.joshworks.ilog.IndexedSegment;
import io.joshworks.ilog.record.RecordsPool;

import java.util.List;

public class ConcatenateCombiner implements SegmentCombiner {

    @Override
    public void merge(List<? extends IndexedSegment> segments, IndexedSegment output) {
        segments.stream()
                .map(RecordsPool::fromSegment)
                .forEach(r -> r.writeTo(output));
    }
}
