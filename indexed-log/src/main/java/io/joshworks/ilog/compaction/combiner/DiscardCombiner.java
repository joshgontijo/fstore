package io.joshworks.ilog.compaction.combiner;

import io.joshworks.ilog.IndexedSegment;
import io.joshworks.ilog.compaction.PeekingIterator;

import java.util.List;

/**
 * Does not merge items from any source segment, leading to the removal of the data and exclusion of segments
 */
public class DiscardCombiner extends MergeCombiner {

    @Override
    public void mergeItems(List<PeekingIterator> items, IndexedSegment output) {
        //do nothing
    }
}
