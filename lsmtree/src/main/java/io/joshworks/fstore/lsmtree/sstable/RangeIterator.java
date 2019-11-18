package io.joshworks.fstore.lsmtree.sstable;

import io.joshworks.fstore.lsmtree.Range;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.SegmentIterator;
import io.joshworks.fstore.lsmtree.sstable.entry.Entry;

class RangeIterator<K extends Comparable<K>, V> implements SegmentIterator<Entry<K, V>> {

    private final Range<K> range;
    private final Direction direction;
    private final SegmentIterator<Entry<K, V>> delegate;
    private Entry<K, V> next = null;

    RangeIterator(SegmentIterator<Entry<K, V>> delegate, Range<K> range, Direction direction) {
        this.delegate = delegate;
        this.range = range;
        this.direction = direction;
    }

    private Entry<K, V> dropWhile() {
        while (delegate.hasNext()) {
            Entry<K, V> entry = delegate.next();
            int compare = range.compareTo(entry.key);
            if (compare == 0) {
                return entry;
            }
            if (Direction.FORWARD.equals(direction) && compare > 0) {
                return null; //GT range, short circuit
            }
            if (Direction.BACKWARD.equals(direction) && compare < 0) {
                return null; //GT range, short circuit
            }
        }
        return null;
    }

    @Override
    public boolean hasNext() {
        if (next != null) {
            return true;
        }
        if (!delegate.hasNext()) {
            return false;
        }
        next = dropWhile();
        return next != null;
    }

    @Override
    public Entry<K, V> next() {
        if (!hasNext()) {
            return null;
        }
        Entry<K, V> tmp = next;
        next = null;
        return tmp;
    }

    @Override
    public boolean endOfLog() {
        return delegate.endOfLog();
    }

    @Override
    public long position() {
        return delegate.position();
    }

    @Override
    public void close() {
        delegate.close();
        next = null;
    }


}
