package io.joshworks.fstore.log.iterators;

import io.joshworks.fstore.log.LogIterator;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collection;
import java.util.NoSuchElementException;
import java.util.Queue;

class StreamingIterator<T> implements LogIterator<T> {

    private final LogIterator<Collection<T>> delegate;
    private Queue<T> buffer = new ArrayDeque<>();

    StreamingIterator(LogIterator<Collection<T>> delegate) {
        this.delegate = delegate;
    }

    @Override
    public long position() {
        return delegate.position();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public boolean hasNext() {
        return !buffer.isEmpty() || streaming();
    }

    @Override
    public T next() {
        if (buffer.isEmpty() && !streaming()) {
            throw new NoSuchElementException();
        }
        return buffer.poll();
    }

    private boolean streaming() {
        if (buffer.isEmpty() && delegate.hasNext()) {
            buffer.addAll(delegate.next());
        }
        return !buffer.isEmpty();
    }
}
