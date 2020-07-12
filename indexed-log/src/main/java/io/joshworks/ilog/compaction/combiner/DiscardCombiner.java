package io.joshworks.ilog.compaction.combiner;

import io.joshworks.ilog.Segment;

import java.util.List;

/**
 * Does not merge items from any source segment, leading to the removal of the data and exclusion of segments
 */
public class DiscardCombiner implements SegmentCombiner {

    @Override
    public void merge(List<? extends Segment> segments, Segment output) {

    }
}
