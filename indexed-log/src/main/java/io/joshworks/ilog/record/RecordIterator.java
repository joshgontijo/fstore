package io.joshworks.ilog.record;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.util.Iterators;

public class RecordIterator implements Iterators.CloseableIterator<Record2> {

    private final Iterators.CloseableIterator<Record2> delegate;
    private boolean hasPeeked;
    private Record2 peekedElement;

    public RecordIterator(Iterators.CloseableIterator<Record2> delegate) {
        this.delegate = delegate;
    }

    @Override
    public boolean hasNext() {
        return hasPeeked || delegate.hasNext();
    }

    @Override
    public Record2 next() {
        if (!hasPeeked) {
            return delegate.next();
        }
        Record2 result = peekedElement;
        peekedElement = null;
        hasPeeked = false;
        return result;
    }

    public Record2 peek() {
        if (!hasPeeked) {
            peekedElement = delegate.next();
            hasPeeked = true;
        }
        return peekedElement;
    }

    @Override
    public void close() {
        try {
            delegate.close();
        } catch (Exception e) {
            throw new RuntimeIOException(e);
        }

    }
}