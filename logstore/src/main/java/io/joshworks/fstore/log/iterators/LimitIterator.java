package io.joshworks.fstore.log.iterators;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.log.LogIterator;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;

class LimitIterator<T> implements LogIterator<T> {

    private final LogIterator<T> delegate;
    private final int limit;
    private final AtomicInteger processed = new AtomicInteger();

    LimitIterator(LogIterator<T> delegate, int limit) {
        this.delegate = delegate;
        this.limit = limit;
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
        if (processed.get() >= limit) {
            IOUtils.closeQuietly(delegate);
            return false;
        }
        return delegate.hasNext();
    }

    @Override
    public T next() {
        if (!hasNext()) {
            IOUtils.closeQuietly(delegate);
            throw new NoSuchElementException();
        }
        processed.incrementAndGet();
        return delegate.next();
    }
}
