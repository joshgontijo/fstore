package io.joshworks.fstore.core.iterators;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

class BatchingIterator<T> implements CloseableIterator<List<T>> {

    private final CloseableIterator<T> delegate;
    private final int bufferSize;
    private List<T> buffer = new ArrayList<>();

    BatchingIterator(CloseableIterator<T> delegate, int bufferSize) {
        this.delegate = delegate;
        this.bufferSize = bufferSize;
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public boolean hasNext() {
        return !buffer.isEmpty() || batch();
    }

    @Override
    public List<T> next() {
        if (buffer.isEmpty() && !batch()) {
            throw new NoSuchElementException();
        }
        List<T> tmp = buffer;
        buffer = new ArrayList<>();
        return tmp;
    }

    private boolean batch() {
        int buffered = buffer.size();
        while (buffered < bufferSize && delegate.hasNext()) {
            buffer.add(delegate.next());
            buffered++;
        }
        return buffered > 0;
    }
}
