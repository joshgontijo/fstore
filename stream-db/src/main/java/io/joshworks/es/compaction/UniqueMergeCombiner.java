package io.joshworks.es.compaction;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

/**
 * For <b>UNIQUE</b> and <b>SORTED</b> segments only.
 * Guaranteed uniqueness only for segments that hold unique items.
 * For items that {@link Comparable#compareTo(Object)} returns equals. The last (newest) item in the list will be used.
 */
public class UniqueMergeCombiner {


    private final List<PeekingIterator<ByteBuffer>> segments;
    private final Comparator<ByteBuffer> comparator;
    private final Consumer<ByteBuffer> out;

    public UniqueMergeCombiner(List<PeekingIterator<ByteBuffer>> segments, Comparator<ByteBuffer> comparator, Consumer<ByteBuffer> out) {
        this.segments = segments;
        this.comparator = comparator;
        this.out = out;
    }

    public void merge() {

        //reversed guarantees that the most recent data is kept when duplicate keys are found
        Collections.reverse(segments);

        while (!segments.isEmpty()) {
            List<PeekingIterator<ByteBuffer>> segmentIterators = new ArrayList<>();
            Iterator<PeekingIterator<ByteBuffer>> itit = segments.iterator();
            while (itit.hasNext()) {
                PeekingIterator<ByteBuffer> seg = itit.next();
                if (!seg.hasNext()) {
                    itit.remove();
                    continue;
                }
                segmentIterators.add(seg);
            }

            //single segment, drain it
            if (segmentIterators.size() == 1) {
                PeekingIterator<ByteBuffer> it = segmentIterators.get(0);
                while (it.hasNext()) {
                    writeOut(it.next());
                }
            } else {
                ByteBuffer nextEntry = getNextEntry(segmentIterators);
                writeOut(nextEntry);
            }
        }
    }

    private void writeOut(ByteBuffer nextEntry) {
        if (nextEntry == null || !filter(nextEntry)) {
            return;
        }
        out.accept(nextEntry);

    }

    private ByteBuffer getNextEntry(List<PeekingIterator<ByteBuffer>> segmentIterators) {
        if (segmentIterators.isEmpty()) {
            return null;
        }
        PeekingIterator<ByteBuffer> prev = null;
        for (PeekingIterator<ByteBuffer> curr : segmentIterators) {
            if (prev == null) {
                prev = curr;
                continue;
            }
            ByteBuffer prevItem = prev.peek();
            ByteBuffer currItem = curr.peek();
            int c = comparator.compare(prevItem, currItem);
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
    public boolean filter(ByteBuffer entry) {
        return true;
    }
}
