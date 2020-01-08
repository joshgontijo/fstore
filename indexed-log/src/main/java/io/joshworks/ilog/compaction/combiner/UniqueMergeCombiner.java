package io.joshworks.ilog.compaction.combiner;

import io.joshworks.ilog.IndexedSegment;
import io.joshworks.ilog.Record;
import io.joshworks.ilog.compaction.PeekingIterator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * For <b>UNIQUE</b> and <b>SORTED</b> segments only.
 * Guaranteed uniqueness only for segments that hold unique items.
 * For items that {@link Comparable#compareTo(Object)} returns equals. The last (newest) item in the list will be used.
 */
public abstract class UniqueMergeCombiner extends MergeCombiner {

    protected abstract int compare(Record r1, Record r2);

    @Override
    public void mergeItems(List<PeekingIterator> items, IndexedSegment output) {

        while (!items.isEmpty()) {
            List<PeekingIterator> segmentIterators = new ArrayList<>();
            //reversed guarantees that the most recent data is kept when duplicate keys are found
            Collections.reverse(items);
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

    /**
     * Returns true if this entry should be appended to the new segment, false otherwise
     */
    public boolean filter(Record entry) {
        return true;
    }
}
