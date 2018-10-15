package io.joshworks.fstore.log.appender.compaction.combiner;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.Iterators;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.segment.Log;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public abstract class MergeCombiner<T, L extends Log<T>> implements SegmentCombiner<T, L> {

    @Override
    public void merge(List<L> segments, L output) {

        List<Iterators.PeekingIterator<T>> iterators = segments.stream()
                .map(s -> s.iterator(Direction.FORWARD))
                .map(Iterators::peekingIterator)
                .collect(Collectors.toList());

        mergeItems(new ArrayList<>(iterators), output);

        for (LogIterator<T> iterator : iterators) {
            IOUtils.closeQuietly(iterator);
        }
    }

    public abstract void mergeItems(List<Iterators.PeekingIterator<T>> items, L output);
}
