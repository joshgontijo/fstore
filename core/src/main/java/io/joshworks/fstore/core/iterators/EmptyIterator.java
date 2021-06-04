package io.joshworks.fstore.core.iterators;

import java.util.NoSuchElementException;

class EmptyIterator<T> implements CloseableIterator<T> {

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public T next() {
        throw new NoSuchElementException();
    }

    @Override
    public void close() {

    }
}
