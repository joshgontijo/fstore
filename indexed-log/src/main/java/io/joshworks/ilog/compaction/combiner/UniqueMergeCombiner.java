package io.joshworks.ilog.compaction.combiner;

import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.ilog.IndexedSegment;
import io.joshworks.ilog.Record;
import io.joshworks.ilog.SegmentIterator;
import io.joshworks.ilog.index.RowKey;
import io.joshworks.ilog.record.RecordPool;
import io.joshworks.ilog.record.Records;

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

    private final RowKey comparator;
    private final BufferPool pool;
    private Records recordBuffer;

    protected UniqueMergeCombiner(RowKey comparator, BufferPool pool) {
        this.comparator = comparator;
        this.pool = pool;
    }

    @Override
    public void merge(List<? extends IndexedSegment> segments, IndexedSegment output) {
        List<SegmentIterator> iterators = segments.stream()
                .map(s -> new SegmentIterator(s, 0, pool))
                .collect(Collectors.toList());

        mergeItems(iterators, output);
    }

    public void mergeItems(List<SegmentIterator> items, IndexedSegment output) {

        //reversed guarantees that the most recent data is kept when duplicate keys are found
        Collections.reverse(items);

        while (!items.isEmpty()) {
            List<SegmentIterator> segmentIterators = new ArrayList<>();
            Iterator<SegmentIterator> itit = items.iterator();
            while (itit.hasNext()) {
                SegmentIterator seg = itit.next();
                if (!seg.hasNext()) {
                    itit.remove();
                    continue;
                }
                segmentIterators.add(seg);
            }

            //single segment, drain it
            if (segmentIterators.size() == 1) {
                SegmentIterator it = segmentIterators.get(0);
                while (it.hasNext()) {
                    writeOut(output, it.next());
                }
            } else {
                ByteBuffer nextEntry = getNextEntry(segmentIterators);
                writeOut(output, nextEntry);
            }
        }
        output.append(recordBuffer, 0);
    }

    private void writeOut(IndexedSegment output, ByteBuffer nextEntry) {
        if (nextEntry == null || !filter(nextEntry)) {
            return;
        }
        if (output.isFull()) {
            throw new IllegalStateException("Insufficient output segment (" + output.name() + ") data space: " + output.size());
        }

        if (recordBuffer.read(nextEntry) == 0) {
            output.append(recordBuffer, 0);
            recordBuffer.close();
            recordBuffer = RecordPool.get("TODO - DEFINE");
        }

    }

    private ByteBuffer getNextEntry(List<SegmentIterator> segmentIterators) {
        if (segmentIterators.isEmpty()) {
            return null;
        }
        SegmentIterator prev = null;
        for (SegmentIterator curr : segmentIterators) {
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
        return Record.compareRecordKeys(r1, r2, comparator);
    }

    /**
     * Returns true if this entry should be appended to the new segment, false otherwise
     */
    public boolean filter(ByteBuffer entry) {
        return true;
    }
}
