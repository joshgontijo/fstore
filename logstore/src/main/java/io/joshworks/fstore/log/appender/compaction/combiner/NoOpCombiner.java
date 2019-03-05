package io.joshworks.fstore.log.appender.compaction.combiner;

import io.joshworks.fstore.log.iterators.PeekingIterator;
import io.joshworks.fstore.log.segment.Log;

import java.util.List;

/**
 * Does not merge items from any source segment, leading to the removal of the data and exclusion of segments
 */
public class NoOpCombiner<T> extends MergeCombiner<T> {

    @Override
    public void mergeItems(List<PeekingIterator<T>> items, Log<T> output) {
        //do nothing
    }
}
