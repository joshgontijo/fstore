package io.joshworks.ilog.compaction.combiner;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.ilog.IndexedSegment;
import io.joshworks.ilog.RecordBatchIterator;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;

public class ConcatenateCombiner implements SegmentCombiner {

    private final BufferPool pool;

    public ConcatenateCombiner(BufferPool pool) {
        this.pool = pool;
    }

    @Override
    public void merge(List<? extends IndexedSegment> segments, IndexedSegment output) {
        List<RecordBatchIterator> iterators = segments.stream()
                .map(s -> new RecordBatchIterator(s, 0, pool))
//                .map(RecordBatchIterator::new)
                .collect(Collectors.toList());

        mergeItems(iterators, output);
    }

    public void mergeItems(List<RecordBatchIterator> items, IndexedSegment output) {
        try {
            for (RecordBatchIterator segmentIterator : items) {
                while (segmentIterator.hasNext()) {
                    ByteBuffer next = segmentIterator.next();
                    if (output.isFull()) {
                        throw new IllegalStateException("Insufficient output segment space: " + output);
                    }
                    output.append(next);
                }
            }
        } catch (Exception e) {
            for (RecordBatchIterator it : items) {
                IOUtils.closeQuietly(it);
            }
            throw e;
        }
    }
}
