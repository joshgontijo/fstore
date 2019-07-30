package io.joshworks.fstore.log.iterators;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.log.CloseableIterator;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

class IteratorIterator<T> implements CloseableIterator<T> {

    private final Iterator<? extends CloseableIterator<T>> it;
    private CloseableIterator<T> current;

    IteratorIterator(Collection<? extends CloseableIterator<T>> iterators) {
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
            IOUtils.closeQuietly(current);
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
    public void close() {
        IOUtils.closeQuietly(current);
        while (it.hasNext()) {
            IOUtils.closeQuietly(it.next());
        }
    }
}
