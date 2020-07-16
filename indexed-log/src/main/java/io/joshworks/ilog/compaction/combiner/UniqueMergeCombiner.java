package io.joshworks.ilog.compaction.combiner;

import io.joshworks.ilog.Segment;
import io.joshworks.ilog.SegmentIterator;
import io.joshworks.ilog.index.RowKey;
import io.joshworks.ilog.record.Record;
import io.joshworks.ilog.record.RecordPool;
import io.joshworks.ilog.record.Records;

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
    private final RowKey rowKey;

    public UniqueMergeCombiner(RecordPool pool, RowKey rowKey) {
        this.pool = pool;
        this.rowKey = rowKey;
    }

    @Override
    public void merge(List<Segment> segments, Segment output) {
        List<SegmentIterator> iterators = segments.stream()
                .map(Segment::iterator)
                .collect(Collectors.toList());

        mergeItems(iterators, output);
    }

    public void mergeItems(List<SegmentIterator> items, Segment output) {

        //reversed guarantees that the most recent data is kept when duplicate keys are found
        Collections.reverse(items);

        try (Records records = pool.empty()) {
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
                        writeOut(records, output, it.next());
                    }
                } else {
                    Record nextEntry = getNextEntry(segmentIterators);
                    writeOut(records, output, nextEntry);
                }
            }
            doWrite(records, output);
        }
    }

    private void doWrite(Records records, Segment output) {
        int copiedItems = output.append(records, 0);
        assert copiedItems == records.size();
        records.clear();
    }

    private void writeOut(Records records, Segment output, Record nextEntry) {
        if (nextEntry == null || !filter(nextEntry)) {
            return;
        }
        if (output.isFull()) {
            throw new IllegalStateException("Insufficient output segment (" + output.name() + ") data space: " + output.size());
        }

        if (!records.add(nextEntry)) {
            doWrite(records, output);
            records.add(nextEntry);
        }

    }

    private Record getNextEntry(List<SegmentIterator> segmentIterators) {
        if (segmentIterators.isEmpty()) {
            return null;
        }
        SegmentIterator prev = null;
        for (SegmentIterator curr : segmentIterators) {
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
        return r1.compare(rowKey, r2);
    }

    /**
     * Returns true if this entry should be appended to the new segment, false otherwise
     */
    public boolean filter(Record entry) {
        return true;
    }
}
