package io.joshworks.ilog.compaction;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.ilog.Record;
import io.joshworks.ilog.SegmentIterator;

import java.io.IOException;

public class PeekingIterator implements SegmentIterator {

    private final SegmentIterator iterator;
    private boolean hasPeeked;
    private Record peekedElement;

    public PeekingIterator(SegmentIterator iterator) {
        this.iterator = iterator;
    }

    @Override
    public boolean hasNext() {
        return hasPeeked || iterator.hasNext();
    }

    @Override
    public Record next() {
        if (!hasPeeked) {
            return iterator.next();
        }
        Record result = peekedElement;
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

    public Record peek() {
        if (!hasPeeked) {
            peekedElement = iterator.next();
            hasPeeked = true;
        }
        return peekedElement;
    }

    @Override
    public void close() {
        try {
            iterator.close();
        } catch (IOException e) {
            throw new RuntimeIOException("Failed to close iterator", e);
        }
    }
}