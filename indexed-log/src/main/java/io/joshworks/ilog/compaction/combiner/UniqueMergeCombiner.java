package io.joshworks.ilog.compaction.combiner;

import io.joshworks.ilog.IndexedSegment;
import io.joshworks.ilog.record.Records;
import io.joshworks.ilog.record.Record2;
import io.joshworks.ilog.record.RecordPool;

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

    private final RecordPool pool;
    private Records recordBuffer;

    protected UniqueMergeCombiner(RecordPool pool) {
        this.pool = pool;
        this.recordBuffer = pool.empty();
    }

    @Override
    public void merge(List<? extends IndexedSegment> segments, IndexedSegment output) {
        List<Records> iterators = segments.stream()
                .map(pool::fromSegment)
                .collect(Collectors.toList());

        mergeItems(iterators, output);
    }

    public void mergeItems(List<Records> items, IndexedSegment output) {

        //reversed guarantees that the most recent data is kept when duplicate keys are found
        Collections.reverse(items);

        while (!items.isEmpty()) {
            List<Records> segmentIterators = new ArrayList<>();
            Iterator<Records> itit = items.iterator();
            while (itit.hasNext()) {
                Records seg = itit.next();
                if (!seg.hasNext()) {
                    itit.remove();
                    continue;
                }
                segmentIterators.add(seg);
            }

            //single segment, drain it
            if (segmentIterators.size() == 1) {
                Records it = segmentIterators.get(0);
                while (it.hasNext()) {
                    writeOut(output, it.next());
                }
            } else {
                Record2 nextEntry = getNextEntry(segmentIterators);
                writeOut(output, nextEntry);
            }
        }
        recordBuffer.writeTo(output);
//        output.append(recordBuffer, 0);
    }

    private void writeOut(IndexedSegment output, Record2 nextEntry) {
        if (nextEntry == null || !filter(nextEntry)) {
            return;
        }
        if (output.index().isFull()) {
            throw new IllegalStateException("Insufficient output segment (" + output.name() + ") data space: " + output.size());
        }

        if (!recordBuffer.add(nextEntry)) {
            recordBuffer.writeTo(output);
            recordBuffer.add(nextEntry);
        }

    }

    private Record2 getNextEntry(List<Records> segmentIterators) {
        if (segmentIterators.isEmpty()) {
            return null;
        }
        Records prev = null;
        for (Records curr : segmentIterators) {
            if (prev == null) {
                prev = curr;
                continue;
            }
            Record2 prevItem = prev.peek();
            Record2 currItem = curr.peek();
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

    private int compare(Record2 r1, Record2 r2) {
        return r1.compareTo(r2);
    }

    /**
     * Returns true if this entry should be appended to the new segment, false otherwise
     */
    public boolean filter(Record2 entry) {
        return true;
    }
}
