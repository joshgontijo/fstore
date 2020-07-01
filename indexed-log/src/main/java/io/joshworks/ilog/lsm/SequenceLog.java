package io.joshworks.ilog.lsm;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.util.FileUtils;
import io.joshworks.ilog.Direction;
import io.joshworks.ilog.FlushMode;
import io.joshworks.ilog.IndexedSegment;
import io.joshworks.ilog.Log;
import io.joshworks.ilog.LogIterator;
import io.joshworks.ilog.Record;
import io.joshworks.ilog.RecordBatch;
import io.joshworks.ilog.SegmentIterator;
import io.joshworks.ilog.index.IndexFunctions;
import io.joshworks.ilog.index.RowKey;
import io.joshworks.ilog.record.Record2;
import io.joshworks.ilog.record.RecordPool;
import io.joshworks.ilog.record.Records;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class SequenceLog implements Closeable {

    public final Log<SequenceSegment> log;
    private final BufferPool keyPool;
    private final AtomicLong sequence = new AtomicLong();
    private final ByteBuffer keyWriteBuffer;
    private final BufferPool pool;

    public SequenceLog(File root,
                       int indexSize,
                       int compactionThreshold,
                       int compactionThreads,
                       FlushMode flushMode,
                       BufferPool pool) throws IOException {
        this.pool = pool;

        FileUtils.createDir(root);
        log = new Log<>(root, indexSize, compactionThreshold, compactionThreads, flushMode, pool, SequenceSegment::new);
        keyPool = BufferPool.defaultPool(256, Long.BYTES, false);
        keyWriteBuffer = keyPool.allocate();
    }

    //returns the first sequence
    public long append(Records records) {
        try {
            long firstSeq = sequence.get();
            try (Records sequenceRecs = RecordPool.get("SEQUENCE_LOG_RECORDS_POOL")) {
                for (Record2 record : records) {
                    long seq = sequence.getAndIncrement();
                    sequenceRecs.wrap(record, b -> b.putLong(seq));
                }
                log.append(sequenceRecs);
                return firstSeq;
            }
        } catch (Exception e) {
            sequence.decrementAndGet();
            throw new RuntimeIOException(e);
        }
    }

    public int bulkRead(long sequence, ByteBuffer dst, IndexFunctions fn) {
        assert sequence >= 0;
        SequenceSegment segment = findSegment(sequence, IndexFunctions.FLOOR);
        if (segment == null) {
            return 0;
        }
        ByteBuffer buffer = longBuffer(sequence);
        try {
            return segment.bulkRead(buffer, dst, fn);
        } finally {
            keyPool.free(buffer);
        }
    }

    public int find(long sequence, ByteBuffer dst, IndexFunctions fn) {
        if (sequence < 0) {
            throw new IllegalArgumentException("Sequence must be greater than zero");
        }

        SequenceSegment segment = findSegment(sequence, IndexFunctions.FLOOR);
        if (segment == null) {
            return 0;
        }
        ByteBuffer buffer = longBuffer(sequence);
        try {
            return segment.find(buffer, dst, fn);
        } finally {
            keyPool.free(buffer);
        }
    }

    private SequenceSegment findSegment(long sequence, IndexFunctions fn) {
        return log.apply(Direction.FORWARD, segs -> findSegment(segs, sequence, fn));
    }

    private static SequenceSegment findSegment(List<SequenceSegment> segments, long sequence, IndexFunctions fn) {
        int idx = indexedBinarySearch(segments, sequence);
        idx = fn.apply(idx);
        if (idx < 0) {
            return null;
        }
        return segments.get(idx);
    }

    private static int indexedBinarySearch(List<SequenceSegment> segments, long sequence) {
        int low = 0;
        int high = segments.size() - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            SequenceSegment midVal = segments.get(mid);
            int cmp = Long.compare(midVal.firstKey(), sequence);

            if (cmp < 0)
                low = mid + 1;
            else if (cmp > 0)
                high = mid - 1;
            else
                return mid; // key found
        }
        return -(low + 1);  // key not found
    }

    private void flush() {
        log.flush();
    }

    @Override
    public void close() {
        log.close();
        keyPool.free(keyWriteBuffer);
    }

    public void delete() {
        log.delete();
    }

    public LogIterator iterator() {
        return log.iterator();
    }

    public LogIterator iterator(long startSequenceInclusive) {
        return log.apply(Direction.FORWARD, segs -> {
            int idx = indexedBinarySearch(segs, startSequenceInclusive);
            idx = IndexFunctions.EQUALS.apply(idx);
            if (idx < 0) {
                return LogIterator.empty();
            }
            long pos = segs.get(idx).positionOf(startSequenceInclusive);
            if (pos < 0) {
                return LogIterator.empty();
            }

            List<SegmentIterator> iterators = new ArrayList<>();
            iterators.add(segs.get(idx).iterator(pos, pool));
            for (int i = idx + 1; i < segs.size(); i++) {
                iterators.add(segs.get(i).iterator(IndexedSegment.START, pool));
            }

            return log.registerIterator(iterators);
        });
    }

    private ByteBuffer longBuffer(long sequence) {
        return keyPool.allocate().putLong(sequence).flip();
    }

    private class SequenceSegment extends IndexedSegment {

        public SequenceSegment(File file, int indexSize) {
            super(file, indexSize, RowKey.LONG);
        }

        public long positionOf(long sequence) {
            var keyBuffer = longBuffer(sequence);
            try {
                index.find(keyBuffer, IndexFunctions.EQUALS);
                keyBuffer.flip();
                if (!keyBuffer.hasRemaining()) {
                    return -1;
                }
                return keyBuffer.getLong();
            } finally {
                keyPool.free(keyBuffer);
            }
        }

        public long firstKey() {
            var keyBuffer = keyPool.allocate();
            try {
                index.first(keyBuffer);
                keyBuffer.flip();
                if (!keyBuffer.hasRemaining()) {
                    return -1;
                }
                return keyBuffer.getLong();
            } finally {
                keyPool.free(keyBuffer);
            }
        }
    }
}
