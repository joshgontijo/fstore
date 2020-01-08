package io.joshworks.ilog.compaction.combiner;

import io.joshworks.ilog.IndexedSegment;

import java.util.List;

public interface SegmentCombiner {

    void merge(List<? extends IndexedSegment> segments, IndexedSegment output);

}
