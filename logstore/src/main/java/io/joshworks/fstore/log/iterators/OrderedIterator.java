package io.joshworks.fstore.log.iterators;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.log.CloseableIterator;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.stream.Collectors;

class OrderedIterator<T, C extends Comparable<C>> implements CloseableIterator<T> {

    private final List<PeekingIterator<T>> iterators;
    private final Function<T, C> mapper;

    OrderedIterator(Collection<? extends CloseableIterator<T>> iterators, Function<T, C> mapper) {
        this.iterators = iterators.stream().map(PeekingIterator::new).collect(Collectors.toList());
        this.mapper = mapper;
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

            C prevItem = mapper.apply(prev.peek());
            C currItem = mapper.apply(curr.peek());
            int c = prevItem.compareTo(currItem);
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
