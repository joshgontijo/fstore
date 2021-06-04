package io.joshworks.fstore.core.iterators;

import java.util.Collection;
import java.util.Iterator;

class ListLogIterator<T> implements CloseableIterator<T> {

    private final Iterator<T> source;
    private int position;

    ListLogIterator(Collection<T> source) {
        this.source = source.iterator();
    }

    @Override
    public void close() {
        //do nothing
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
