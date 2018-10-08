package io.joshworks.fstore.log.appender.compaction.combiner;

import io.joshworks.fstore.log.Iterators;
import io.joshworks.fstore.log.segment.Log;

import java.util.List;

public class ConcatenateCombiner<T> extends MergeCombiner<T> {

    @Override
    public void mergeItems(List<Iterators.PeekingIterator<T>> items, Log<T> output) {
        items.stream().flatMap(Iterators::closeableStream).forEach(output::append);
    }
}
