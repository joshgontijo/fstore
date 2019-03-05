package io.joshworks.fstore.log.appender.compaction.combiner;

import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.iterators.Iterators;
import io.joshworks.fstore.log.iterators.PeekingIterator;
import io.joshworks.fstore.log.segment.Log;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * For <b>UNIQUE</b> and <b>SORTED</b> segments only.
 * Guaranteed uniqueness only for segments that holds unique items.
 * For items that {@link Comparable#compareTo(Object)} returns equals. The last (newest) item in the list will be used.
 */
public class UniqueMergeCombiner<T extends Comparable<T>> extends MergeCombiner<T> {

    @Override
    public void mergeItems(List<PeekingIterator<T>> items, Log<T> output) {

        while (!items.isEmpty()) {
            List<PeekingIterator<T>> segmentIterators = new ArrayList<>();
            //reversed guarantees that the most recent data is kept when duplicate keys are found
            Iterator<PeekingIterator<T>> itit = Iterators.reversed(items);
            while (itit.hasNext()) {
                PeekingIterator<T> seg = itit.next();
                if (!seg.hasNext()) {
                    itit.remove();
                    continue;
                }
                segmentIterators.add(seg);
            }

            T nextEntry = getNextEntry(segmentIterators);
            if (nextEntry != null && filter(nextEntry)) {
                long pos = output.append(nextEntry);
                if (pos == Storage.EOF) {
                    throw new IllegalStateException("Insufficient output segment space: " + output.fileSize());
                }
            }
        }
    }

    private T getNextEntry(List<PeekingIterator<T>> segmentIterators) {
        if (segmentIterators.isEmpty()) {
            return null;
        }
        PeekingIterator<T> prev = null;
        for (PeekingIterator<T> curr : segmentIterators) {
            if (prev == null) {
                prev = curr;
                continue;
            }
            T prevItem = prev.peek();
            T currItem = curr.peek();
            int c = prevItem.compareTo(currItem);
            if (c == 0) { //duplicate remove form oldest entry
                prev.next();
            }
            if (c >= 0) {
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
    public boolean filter(T entry) {
        return true;
    }
}
