package io.joshworks.fstore.log.appender.compaction.combiner;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.Iterators;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.segment.Log;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public abstract class MergeCombiner<T extends Comparable<T>> implements SegmentCombiner<T> {

    @Override
    public void merge(List<? extends Log<T>> segments, Log<T> output) {

        List<Iterators.PeekingIterator<T>> iterators = segments.stream()
                .map(s -> s.iterator(Direction.FORWARD))
                .map(Iterators::peekingIterator)
                .collect(Collectors.toList());

        mergeItems(new ArrayList<>(iterators), output);

        for (LogIterator<T> iterator : iterators) {
            IOUtils.closeQuietly(iterator);
        }
    }

    public abstract void mergeItems(List<Iterators.PeekingIterator<T>> items, Log<T> output);
}
