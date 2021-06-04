package io.joshworks.fstore.core.iterators;

public class PeekingIterator<E> implements CloseableIterator<E> {

    private final CloseableIterator<? extends E> iterator;
    private boolean hasPeeked;
    private E peekedElement;

    public PeekingIterator(CloseableIterator<? extends E> iterator) {
        this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
        return hasPeeked || iterator.hasNext();
    }

    @Override
    public E next() {
        if (!hasPeeked) {
            return iterator.next();
        }
        E result = peekedElement;
        peekedElement = null;
        hasPeeked = false;
        return result;
    }

    @Override
    public void remove() {
        if (!hasPeeked) {
            throw new IllegalStateException("Can't remove after you've peeked at next");
        }
        iterator.remove();
    }

    public E peek() {
        if (!hasPeeked) {
            peekedElement = iterator.next();
            hasPeeked = true;
        }
        return peekedElement;
    }

    @Override
    public void close() {
        iterator.close();
    }
}
