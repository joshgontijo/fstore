package io.joshworks.fstore.log.appender.compaction.combiner;

import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.segment.Log;

import java.util.List;

public class ConcatenateCombiner<T> implements SegmentCombiner<T> {

    @Override
    public void merge(List<? extends Log<T>> segments, Log<T> output) {
        segments.stream()
                .map(s -> s.stream(Direction.FORWARD))
                .flatMap(l -> l)
                .forEach(output::append);
    }
}