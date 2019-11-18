package io.joshworks.fstore.lsmtree.sstable;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.log.CloseableIterator;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.iterators.PeekingIterator;
import io.joshworks.fstore.lsmtree.sstable.entry.Entry;

import java.util.Iterator;
import java.util.List;

class SSTablesIterator<K extends Comparable<K>, V> implements CloseableIterator<Entry<K, V>> {

    private final Direction direction;
    private final List<PeekingIterator<Entry<K, V>>> segmentsIterators;

    SSTablesIterator(Direction direction, List<PeekingIterator<Entry<K, V>>> iterators) {
        this.direction = direction;
        this.segmentsIterators = iterators;
    }

    @Override
    public Entry<K, V> next() {
        if (!segmentsIterators.isEmpty()) {
            PeekingIterator<Entry<K, V>> smaller = null;
            Iterator<PeekingIterator<Entry<K, V>>> itit = segmentsIterators.iterator();
            while (itit.hasNext()) {
                PeekingIterator<Entry<K, V>> curr = itit.next();
                if (!curr.hasNext()) {
                    itit.remove();
                    IOUtils.closeQuietly(curr);
                    continue;
                }
                if (smaller == null) {
                    smaller = curr;
                    continue;
                }
                Entry<K, V> smallerEntry = smaller.peek();
                Entry<K, V> currItem = curr.peek();
                int c = smallerEntry.compareTo(currItem);
                if (c == 0) { //duplicate remove from oldest entry
                    smaller.next();
                    if (Direction.BACKWARD.equals(direction)) {
                        smaller = curr;
                    }
                }
                if ((Direction.FORWARD.equals(direction)) && c >= 0 || (Direction.BACKWARD.equals(direction) && c < 0)) {
                    smaller = curr;
                }
            }
            if (smaller != null) {
                return smaller.next();
            }
        }
        return null;
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

}
