package io.joshworks.ilog.compaction.combiner;

import io.joshworks.ilog.Segment;

import java.util.List;

public interface SegmentCombiner {

    void merge(List<Segment> segments, Segment output);

}
