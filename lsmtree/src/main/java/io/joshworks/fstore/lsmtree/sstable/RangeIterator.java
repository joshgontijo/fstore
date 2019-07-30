package io.joshworks.fstore.lsmtree.sstable;

import io.joshworks.fstore.index.Range;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.SegmentIterator;

import java.io.IOException;

public class RangeIterator<K extends Comparable<K>, V> extends SSTableIterator<K, V> {

    private final Range<K> range;
    private final Direction direction;
    private Entry<K, V> next = null;

    public RangeIterator(long maxAge, SegmentIterator<Entry<K, V>> delegate, Range<K> range, Direction direction) {
        super(maxAge, delegate);
        this.range = range;
        this.direction = direction;
    }

    private Entry<K, V> dropWhile() {
        while (super.hasNext()) {
            Entry<K, V> entry = super.next();
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
        if (!super.hasNext()) {
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
        return super.endOfLog();
    }

    @Override
    public long position() {
        return super.position();
    }

    @Override
    public void close() {
        super.close();
    }


}
