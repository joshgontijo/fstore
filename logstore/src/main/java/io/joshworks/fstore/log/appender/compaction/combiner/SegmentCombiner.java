package io.joshworks.fstore.log.appender.compaction.combiner;

import io.joshworks.fstore.log.segment.Log;

import java.util.List;

public interface SegmentCombiner<T, L extends Log<T>> {

    void merge(List<L> segments, L output);

}
