package io.joshworks.fstore.log.iterators;

import io.joshworks.fstore.log.LogIterator;

import java.io.IOException;

public class PeekingIterator<E> implements LogIterator<E> {

    private final LogIterator<? extends E> iterator;
    private boolean hasPeeked;
    private E peekedElement;

    public PeekingIterator(LogIterator<? extends E> iterator) {
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
    public long position() {
        return iterator.position();
    }

    @Override
    public void close() throws IOException {
        iterator.close();
    }
}
