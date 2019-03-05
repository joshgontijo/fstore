package io.joshworks.fstore.log.appender.compaction.combiner;

import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.iterators.Iterators;
import io.joshworks.fstore.log.iterators.PeekingIterator;
import io.joshworks.fstore.log.segment.Log;

import java.util.Iterator;
import java.util.List;

public class ConcatenateCombiner<T> extends MergeCombiner<T> {

    @Override
    public void mergeItems(List<PeekingIterator<T>> items, Log<T> output) {
        Iterator<T> it = items.stream().flatMap(Iterators::closeableStream).iterator();
        while (it.hasNext()) {
            T next = it.next();
            long pos = output.append(next);
            if (pos == Storage.EOF) {
                throw new IllegalStateException("Insufficient output segment space: " + output);
            }
        }
    }
}
