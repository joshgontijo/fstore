package io.joshworks.fstore.log.appender.compaction.combiner;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.iterators.PeekingIterator;
import io.joshworks.fstore.log.segment.Log;

import java.util.List;

public class ConcatenateCombiner<T> extends MergeCombiner<T> {

    @Override
    public void mergeItems(List<PeekingIterator<T>> items, Log<T> output) {
        try {
            for (PeekingIterator<T> segmentIterator : items) {
                while (segmentIterator.hasNext()) {
                    T next = segmentIterator.next();
                    long pos = output.append(next);
                    if (pos == Storage.EOF) {
                        throw new IllegalStateException("Insufficient output segment space: " + output);
                    }
                }
            }
        } catch (Exception e) {
            for (PeekingIterator<T> it : items) {
                IOUtils.closeQuietly(it);
            }
        }
    }
}
