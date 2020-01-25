package io.joshworks.ilog.compaction.combiner;

import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.ilog.IndexedSegment;
import io.joshworks.ilog.Record2;
import io.joshworks.ilog.RecordBatchIterator;
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
        List<RecordBatchIterator> iterators = segments.stream()
                .map(s -> new RecordBatchIterator(s, 0, pool))
                .collect(Collectors.toList());

        mergeItems(iterators, output);
    }


    public void mergeItems(List<RecordBatchIterator> items, IndexedSegment output) {

        //reversed guarantees that the most recent data is kept when duplicate keys are found
        Collections.reverse(items);

        while (!items.isEmpty()) {
            List<RecordBatchIterator> segmentIterators = new ArrayList<>();
            Iterator<RecordBatchIterator> itit = items.iterator();
            while (itit.hasNext()) {
                RecordBatchIterator seg = itit.next();
                if (!seg.hasNext()) {
                    itit.remove();
                    continue;
                }
                segmentIterators.add(seg);
            }

            //single segment, drain it
            if (segmentIterators.size() == 1) {
                RecordBatchIterator it = segmentIterators.get(0);
                while (it.hasNext()) {
                    writeOut(output, it.next());
                }
            } else {
                ByteBuffer nextEntry = getNextEntry(segmentIterators);
                writeOut(output, nextEntry);
            }

        }
    }

    private void writeOut(IndexedSegment output, ByteBuffer nextEntry) {
        if (nextEntry == null || !filter(nextEntry)) {
            return;
        }
        if (output.isFull()) {
            throw new IllegalStateException("Insufficient output segment (" + output.name() + ") data space: " + output.size());
        }
        output.append(nextEntry);
    }

    private ByteBuffer getNextEntry(List<RecordBatchIterator> segmentIterators) {
        if (segmentIterators.isEmpty()) {
            return null;
        }
        RecordBatchIterator prev = null;
        for (RecordBatchIterator curr : segmentIterators) {
            if (prev == null) {
                prev = curr;
                continue;
            }
            ByteBuffer prevItem = prev.peek();
            ByteBuffer currItem = curr.peek();
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

    private int compare(ByteBuffer r1, ByteBuffer r2) {
        return Record2.compareRecordKeys(r1, r2, comparator);
    }

    /**
     * Returns true if this entry should be appended to the new segment, false otherwise
     */
    public boolean filter(ByteBuffer entry) {
        return true;
    }
}
