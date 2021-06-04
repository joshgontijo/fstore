package io.joshworks.fstore.core.iterators;

import java.util.List;
import java.util.NoSuchElementException;

public class CycleIterator<T> implements CloseableIterator<T> {

    private final List<T> iterable;
    private CloseableIterator<T> iterator = Iterators.empty();

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
    public void close() {

    }
}