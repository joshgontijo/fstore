package io.joshworks.fstore.log.iterators;

import io.joshworks.fstore.log.LogIterator;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

class IteratorIterator<T> implements LogIterator<T> {

    private final Iterator<? extends LogIterator<T>> it;
    private LogIterator<T> current;

    IteratorIterator(Collection<? extends LogIterator<T>> iterators) {
        this.it = iterators.iterator();
        nextIterator();
    }

    @Override
    public boolean hasNext() {
        nextIterator();
        boolean hasNext = current != null && current.hasNext();
        if (!hasNext) {
            this.close();
        }
        return hasNext;
    }

    private void nextIterator() {
        while ((current == null || !current.hasNext()) && it.hasNext()) {
            closeIterator(current);
            current = it.next();
        }
    }

    @Override
    public T next() {
        if (!hasNext()) {
            close();
            throw new NoSuchElementException();
        }

        return current.next();
    }

    @Override
    public long position() {
        return current.position();
    }

    @Override
    public void close() {
        closeIterator(current);
        while (it.hasNext()) {
            closeIterator(it.next());
        }
    }

    private void closeIterator(LogIterator<T> it) {
        if (it == null) {
            return;
        }
        try {
            it.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
