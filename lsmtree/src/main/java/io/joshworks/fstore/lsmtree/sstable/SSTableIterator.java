package io.joshworks.fstore.lsmtree.sstable;

import io.joshworks.fstore.log.SegmentIterator;

import java.io.IOException;


public class SSTableIterator<K extends Comparable<K>, V> implements SegmentIterator<Entry<K, V>> {

    private final long maxAge;
    private final SegmentIterator<Entry<K, V>> delegate;
    private Entry<K, V> next;

    SSTableIterator(long maxAge, SegmentIterator<Entry<K, V>> delegate) {
        this.maxAge = maxAge;
        this.delegate = delegate;
    }

    @Override
    public boolean hasNext() {
        next = takeWhile();
        return next != null;
    }

    //guaranteed to return an element if hasNext evaluated to true
    @Override
    public Entry<K, V> next() {
        Entry<K, V> record = takeWhile();
        next = null;
        return record;
    }

    private Entry<K, V> takeWhile() {
        if (next != null) {
            return next;
        }
        Entry<K, V> last = nextEntry();
        while (last != null && !last.readable(maxAge)) {
            last = nextEntry();
        }
        return last != null && last.readable(maxAge) ? last : null;
    }

    private Entry<K, V> nextEntry() {
        return delegate.hasNext() ? delegate.next() : null;
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
    public boolean endOfLog() {
        return delegate.endOfLog();
    }
}