package io.joshworks.fstore.core.iterators;

import io.joshworks.fstore.core.io.IOUtils;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

class MergeSortIterator<T> implements CloseableIterator<T> {

    private final List<PeekingIterator<T>> iterators;
    private final Comparator<T> cmp;

    MergeSortIterator(List<PeekingIterator<T>> iterators, Comparator<T> cmp) {
        this.iterators = iterators;
        this.cmp = cmp;
    }

    @Override
    public boolean hasNext() {
        for (PeekingIterator<T> next : iterators) {
            if (next.hasNext()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public T next() {
        if (iterators.isEmpty()) {
            throw new NoSuchElementException();
        }
        Iterator<PeekingIterator<T>> itit = iterators.iterator();
        PeekingIterator<T> prev = null;
        while (itit.hasNext()) {
            PeekingIterator<T> curr = itit.next();
            if (!curr.hasNext()) {
                itit.remove();
                continue;
            }
            if (prev == null) {
                prev = curr;
                continue;
            }

            T prevItem = prev.peek();
            T currItem = curr.peek();
            int c = cmp.compare(prevItem, currItem);
            prev = c >= 0 ? curr : prev;
        }
        if (prev != null) {
            return prev.next();
        }
        return null;
    }

    @Override
    public void close() {
        iterators.forEach(IOUtils::closeQuietly);
    }
}
