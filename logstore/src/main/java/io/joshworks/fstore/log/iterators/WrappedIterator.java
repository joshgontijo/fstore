package io.joshworks.fstore.log.iterators;

import io.joshworks.fstore.log.CloseableIterator;
import io.joshworks.fstore.log.LogIterator;

import java.util.Iterator;

class WrappedIterator<T> implements CloseableIterator<T> {

    private final Iterator<T> delegate;

    WrappedIterator(Iterator<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void close() {
        //do nothing
    }

    @Override
    public boolean hasNext() {
        return delegate.hasNext();
    }

    @Override
    public T next() {
        return delegate.next();
    }
}
