package io.joshworks.fstore.log.iterators;

import io.joshworks.fstore.log.LogIterator;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class CycleIterator<T> implements LogIterator<T> {

    private final List<T> iterable;
    private LogIterator<T> iterator = Iterators.empty();

    public CycleIterator(List<T> iterable) {
        this.iterable = iterable;
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext() || iterable.iterator().hasNext();
    }

    @Override
    public T next() {
        if (!iterator.hasNext()) {
            iterator = Iterators.of(iterable);
            if (!iterator.hasNext()) {
                throw new NoSuchElementException();
            }
        }
        return iterator.next();
    }

    @Override
    public long position() {
        return 0;
    }

    @Override
    public void close() {

    }
}