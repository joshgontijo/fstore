package io.joshworks.fstore.log.iterators;

import io.joshworks.fstore.log.LogIterator;

import java.util.List;
import java.util.ListIterator;

class ReversedIterator<T> implements LogIterator<T> {
    final ListIterator<T> i;

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
    public long position() {
        //do nothing
        return -1;
    }

    @Override
    public void close() {
        //do nothing
    }
}
