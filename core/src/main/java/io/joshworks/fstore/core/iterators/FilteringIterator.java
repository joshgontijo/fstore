package io.joshworks.fstore.core.iterators;

import java.util.NoSuchElementException;
import java.util.function.Predicate;

class FilteringIterator<T> implements CloseableIterator<T> {

    private final CloseableIterator<T> delegate;
    private Predicate<? super T> predicate;
    private T entry;

    FilteringIterator(CloseableIterator<T> delegate, Predicate<? super T> predicate) {
        this.delegate = delegate;
        this.predicate = predicate;
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public boolean hasNext() {
        if (entry == null) {
            entry = takeWhile();
            return entry != null;
        }
        return true;
    }

    @Override
    public T next() {
        if (entry == null) {
            entry = takeWhile();
            if (entry == null) {
                throw new NoSuchElementException();
            }
        }
        T tmp = entry;
        entry = null;
        return tmp;
    }

    private T takeWhile() {
        T match = null;
        do {
            if (delegate.hasNext()) {
                T next = delegate.next();
                match = predicate.test(next) ? next : null;

            }
        } while (match == null && delegate.hasNext());
        return match;
    }
}
