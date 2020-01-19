package io.joshworks.ilog.compaction.combiner;

import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.ilog.IndexedSegment;
import io.joshworks.ilog.Record;
import io.joshworks.ilog.RecordBatchIterator;
import io.joshworks.ilog.compaction.PeekingIterator;
import io.joshworks.ilog.index.KeyComparator;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * For <b>UNIQUE</b> and <b>SORTED</b> segments only.
 * Guaranteed uniqueness only for segments that hold unique items.
 * For items that {@link Comparable#compareTo(Object)} returns equals. The last (newest) item in the list will be used.
 */
public class UniqueMergeCombiner implements SegmentCombiner {

    private final KeyComparator comparator;
    private final ByteBuffer k1Buffer;
    private final ByteBuffer k2Buffer;
    private final BufferPool pool;

    protected UniqueMergeCombiner(KeyComparator comparator, BufferPool pool) {
        this.comparator = comparator;
        this.k1Buffer = Buffers.allocate(comparator.keySize(), false);
        this.k2Buffer = Buffers.allocate(comparator.keySize(), false);
        this.pool = pool;
    }

    @Override
    public void merge(List<? extends IndexedSegment> segments, IndexedSegment output) {
        List<PeekingIterator> iterators = segments.stream()
                .map(s -> new RecordBatchIterator(s, 0, pool))
                .map(PeekingIterator::new)
                .collect(Collectors.toList());

        mergeItems(iterators, output);
    }


    public void mergeItems(List<PeekingIterator> items, IndexedSegment output) {

        //reversed guarantees that the most recent data is kept when duplicate keys are found
        Collections.reverse(items);

        while (!items.isEmpty()) {
            List<PeekingIterator> segmentIterators = new ArrayList<>();
            Iterator<PeekingIterator> itit = items.iterator();
            while (itit.hasNext()) {
                PeekingIterator seg = itit.next();
                if (!seg.hasNext()) {
                    itit.remove();
                    continue;
                }
                segmentIterators.add(seg);
            }

            Record nextEntry = getNextEntry(segmentIterators);
            if (nextEntry != null && filter(nextEntry)) {
                if (output.isFull()) {
                    throw new IllegalStateException("Insufficient output segment (" + output.name() + ") data space: " + output.size());
                }
                output.append(nextEntry);
            }
        }
    }

    private Record getNextEntry(List<PeekingIterator> segmentIterators) {
        if (segmentIterators.isEmpty()) {
            return null;
        }
        PeekingIterator prev = null;
        for (PeekingIterator curr : segmentIterators) {
            if (prev == null) {
                prev = curr;
                continue;
            }
            Record prevItem = prev.peek();
            Record currItem = curr.peek();
            int c = compare(prevItem, currItem);
            if (c == 0) { //duplicate remove eldest entry
                curr.next();
            }
            if (c > 0) {
                prev = curr;
            }
        }
        if (prev != null) {
            return prev.next();
        }
        return null;
    }

    private int compare(Record r1, Record r2) {
        r1.readKey(k1Buffer);
        k1Buffer.flip();

        r2.readKey(k2Buffer);
        k2Buffer.flip();

        int compare = comparator.compare(k1Buffer, k2Buffer);
        k1Buffer.clear();
        k2Buffer.clear();

        return compare;
    }

    /**
     * Returns true if this entry should be appended to the new segment, false otherwise
     */
    public boolean filter(Record entry) {
        return true;
    }
}
