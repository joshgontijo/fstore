package io.joshworks.fstore.log.iterators;

import io.joshworks.fstore.log.LogIterator;

import java.util.Collection;
import java.util.Iterator;

class ListLogIterator<T> implements LogIterator<T> {

    private final Iterator<T> source;
    private int position;

    ListLogIterator(Collection<T> source) {
        this.source = source.iterator();
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
    public void remove() {
        source.remove();
    }

    @Override
    public boolean hasNext() {
        return source.hasNext();
    }

    @Override
    public T next() {
        T next = source.next();
        position++;
        return next;
    }
}
