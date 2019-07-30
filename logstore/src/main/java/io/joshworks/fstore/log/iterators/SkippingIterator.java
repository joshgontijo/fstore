package io.joshworks.fstore.log.iterators;

import io.joshworks.fstore.log.LogIterator;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

class SkippingIterator<T> implements LogIterator<T> {

    private final LogIterator<T> delegate;
    private final int skips;
    private final AtomicInteger skipped = new AtomicInteger();

    SkippingIterator(LogIterator<T> delegate, int skips) {
        this.delegate = delegate;
        this.skips = skips;
    }

    @Override
    public long position() {
        return delegate.position();
    }

    @Override
    public void close() {
        delegate.close();
    }

    @Override
    public boolean hasNext() {
        if (skipped.get() < skips) {
            skip();
        }
        return delegate.hasNext();
    }

    @Override
    public T next() {
        if (skipped.get() < skips) {
            skip();
        }
        return delegate.next();
    }

    private void skip() {
        while (skipped.get() < skips && delegate.hasNext()) {
            delegate.next();
            skipped.incrementAndGet();
        }
    }


}
