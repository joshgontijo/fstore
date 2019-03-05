package io.joshworks.fstore.log.iterators;

import io.joshworks.fstore.log.LogIterator;

import java.util.NoSuchElementException;

class EmptyIterator<T> implements LogIterator<T> {

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public T next() {
        throw new NoSuchElementException();
    }

    @Override
    public long position() {
        return 0;
    }

    @Override
    public void close() {

    }
}
