package io.joshworks.fstore.log.iterators;

import io.joshworks.fstore.log.CloseableIterator;

import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Queue;

class BufferingIterator<T> implements CloseableIterator<T> {

    private final CloseableIterator<T> delegate;
    private final int bufferSize;
    private final Queue<T> buffer = new LinkedList<>();

    BufferingIterator(CloseableIterator<T> delegate, int bufferSize) {
        this.delegate = delegate;
        this.bufferSize = bufferSize;
    }

    @Override
    public void close()  {
        delegate.close();
    }

    @Override
    public boolean hasNext() {
        return !buffer.isEmpty() || buffer();
    }

    @Override
    public T next() {
        if (buffer.isEmpty() && !buffer()) {
            throw new NoSuchElementException();
        }
        return buffer.poll();
    }

    private boolean buffer() {
        int buffered = buffer.size();
        while (buffered < bufferSize && delegate.hasNext()) {
            buffer.add(delegate.next());
            buffered++;
        }
        return buffered > 0;
    }
}
