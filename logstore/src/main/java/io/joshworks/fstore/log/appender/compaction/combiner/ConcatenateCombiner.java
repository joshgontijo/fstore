package io.joshworks.fstore.log.appender.compaction.combiner;

import io.joshworks.fstore.log.Iterators;
import io.joshworks.fstore.log.segment.Log;

import java.util.List;

public class ConcatenateCombiner<T, L extends Log<T>> extends MergeCombiner<T, L> {

    @Override
    public void mergeItems(List<Iterators.PeekingIterator<T>> items, L output) {
        items.stream().flatMap(Iterators::closeableStream).forEach(output::append);
    }
}
