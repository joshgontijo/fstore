package io.joshworks.fstore.log.appender.compaction.combiner;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.log.CloseableIterator;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.iterators.Iterators;
import io.joshworks.fstore.log.iterators.PeekingIterator;
import io.joshworks.fstore.log.segment.Log;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public abstract class MergeCombiner<T> implements SegmentCombiner<T> {

    @Override
    public void merge(List<? extends Log<T>> segments, Log<T> output) {

        List<PeekingIterator<T>> iterators = segments.stream()
                .map(s -> s.iterator(Direction.FORWARD))
                .map(Iterators::peekingIterator)
                .collect(Collectors.toList());

        try {
            mergeItems(new ArrayList<>(iterators), output);
        } finally {
            for (CloseableIterator<T> iterator : iterators) {
                IOUtils.closeQuietly(iterator);
            }
        }

    }

    public abstract void mergeItems(List<PeekingIterator<T>> items, Log<T> output);
}
