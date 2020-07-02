package io.joshworks.ilog.compaction.combiner;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.ilog.IndexedSegment;
import io.joshworks.ilog.SegmentIterator;
import io.joshworks.ilog.record.RecordPool;
import io.joshworks.ilog.record.BufferRecords;

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
        List<SegmentIterator> iterators = segments.stream()
                .map(s -> s.iterator(0, pool))
                .collect(Collectors.toList());

        mergeItems(iterators, output);
    }

    public void mergeItems(List<SegmentIterator> items, IndexedSegment output) {
        try {

            BufferRecords records = RecordPool.get("TODO - DEFINE");

            for (SegmentIterator segmentIterator : items) {
                while (segmentIterator.hasNext()) {
                    ByteBuffer next = segmentIterator.next();
                    if (output.isFull()) {
                        throw new IllegalStateException("Insufficient output segment space: " + output);
                    }

                    if(records.fromBuffer(next) == 0) {
                        output.append(records, 0);
                        records.close();
                        records = RecordPool.get("TODO - DEFINE");
                    }

                }
            }
        } catch (Exception e) {
            for (SegmentIterator it : items) {
                IOUtils.closeQuietly(it);
            }
            throw e;
        }
    }
}
