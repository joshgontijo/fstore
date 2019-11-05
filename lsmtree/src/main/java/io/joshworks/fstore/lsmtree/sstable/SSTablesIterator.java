package io.joshworks.fstore.lsmtree.sstable;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.log.CloseableIterator;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.iterators.PeekingIterator;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

class SSTablesIterator<K extends Comparable<K>, V> implements CloseableIterator<Entry<K, V>> {

    private final long maxAge;
    private final Direction direction;
    private final List<PeekingIterator<Entry<K, V>>> segmentsIterators;

    SSTablesIterator(long maxAge, Direction direction, List<PeekingIterator<Entry<K, V>>> iterators) {
        this.maxAge = maxAge;
        this.direction = direction;
        this.segmentsIterators = iterators;
    }

    @Override
    public Entry<K, V> next() {
        Entry<K, V> entry;
        do {
            entry = getNextEntry(direction, segmentsIterators);
        } while (entry != null && !entry.readable(maxAge) && hasNext());
        if (entry == null) {
            throw new NoSuchElementException();
        }
        return entry;
    }

    @Override
    public void close() {
        for (PeekingIterator<Entry<K, V>> availableSegment : segmentsIterators) {
            availableSegment.close();
        }
    }

    @Override
    public boolean hasNext() {
        for (PeekingIterator<Entry<K, V>> segment : segmentsIterators) {
            if (segment.hasNext()) {
                return true;
            }
        }
        return false;
    }

    private Entry<K, V> getNextEntry(Direction direction, List<PeekingIterator<Entry<K, V>>> segmentIterators) {
        if (!segmentIterators.isEmpty()) {
            PeekingIterator<Entry<K, V>> prev = null;
            Iterator<PeekingIterator<Entry<K, V>>> itit = segmentIterators.iterator();
            while (itit.hasNext()) {
                PeekingIterator<Entry<K, V>> curr = itit.next();
                if (!curr.hasNext()) {
                    itit.remove();
                    IOUtils.closeQuietly(curr);
                    continue;
                }
                if (prev == null) {
                    prev = curr;
                    continue;
                }
                Entry<K, V> prevItem = prev.peek();
                Entry<K, V> currItem = curr.peek();
                int c = prevItem.compareTo(currItem);
                if (c == 0) { //duplicate remove from oldest entry
                    prev.next();
                    if (Direction.BACKWARD.equals(direction)) {
                        prev = curr;
                    }
                }
                if ((Direction.FORWARD.equals(direction)) && c >= 0 || (Direction.BACKWARD.equals(direction) && c < 0)) {
                    prev = curr;
                }
            }
            if (prev != null) {
                return prev.next();
            }
        }
        return null;
    }
}
