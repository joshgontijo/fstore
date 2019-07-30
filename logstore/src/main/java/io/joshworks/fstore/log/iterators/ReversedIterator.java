package io.joshworks.fstore.log.iterators;

import io.joshworks.fstore.log.CloseableIterator;
import io.joshworks.fstore.log.LogIterator;

import java.util.List;
import java.util.ListIterator;

class ReversedIterator<T> implements CloseableIterator<T> {
    private final ListIterator<T> i;

    ReversedIterator(List<T> original) {
        this.i = original.listIterator(original.size());
    }

    public boolean hasNext() {
        return i.hasPrevious();
    }

    public T next() {
        return i.previous();
    }

    public void remove() {
        i.remove();
    }

    @Override
    public void close() {
        //do nothing
    }
}
