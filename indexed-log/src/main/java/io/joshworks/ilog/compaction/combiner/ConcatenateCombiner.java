package io.joshworks.ilog.compaction.combiner;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.ilog.IndexedSegment;
import io.joshworks.ilog.Record;
import io.joshworks.ilog.compaction.PeekingIterator;

import java.util.List;

public class ConcatenateCombiner extends MergeCombiner {

    @Override
    public void mergeItems(List<PeekingIterator> items, IndexedSegment output) {
        try {
            for (PeekingIterator segmentIterator : items) {
                while (segmentIterator.hasNext()) {
                    Record next = segmentIterator.next();
                    if (output.isFull()) {
                        throw new IllegalStateException("Insufficient output segment space: " + output);
                    }
                    output.append(next);
                }
            }
        } catch (Exception e) {
            for (PeekingIterator it : items) {
                IOUtils.closeQuietly(it);
            }
            throw e;
        }
    }
}
