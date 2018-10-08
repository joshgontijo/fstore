package io.joshworks.fstore.log.appender.compaction.combiner;

import io.joshworks.fstore.log.segment.Log;

import java.util.List;

/**
 * Does not merge items from any source segment, leading to the removal of the data and exclusion of segments
 */
public class NoOpCombiner<T> implements SegmentCombiner<T> {

    @Override
    public void merge(List<? extends Log<T>> segments, Log<T> output) {

    }
}
