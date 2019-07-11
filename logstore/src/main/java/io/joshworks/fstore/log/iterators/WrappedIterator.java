package io.joshworks.fstore.log.iterators;

import io.joshworks.fstore.log.LogIterator;

import java.util.Iterator;

class WrappedIterator<T> implements LogIterator<T> {

    private int position;
    private final Iterator<T> delegate;

    WrappedIterator(Iterator<T> delegate) {
        this.delegate = delegate;
    }

    @Override
    public long position() {
        return position;
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
        T next = delegate.next();
        position++;
        return next;
    }
}
