package io.joshworks.ilog.lsm;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.util.FileUtils;
import io.joshworks.ilog.Direction;
import io.joshworks.ilog.FlushMode;
import io.joshworks.ilog.IndexedSegment;
import io.joshworks.ilog.Log;
import io.joshworks.ilog.Record;
import io.joshworks.ilog.index.IndexFunctions;
import io.joshworks.ilog.index.KeyComparator;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class SequenceLog implements Closeable {

    private final Log<SequenceSegment> log;
    private final BufferPool keyPool;
    private final AtomicLong sequence = new AtomicLong();
    private final ByteBuffer keyWriteBuffer;
    private final ByteBuffer recordWriteBuffer;

    //File root,
    //               int maxEntrySize,
    //               int indexSize,
    //               int compactionThreshold,
    //               int parallelCompaction,
    //               FlushMode flushMode,
    //               BufferPool pool,
    //               SegmentFactory<T> segmentFactory

    public SequenceLog(File root,
                       int maxEntrySize,
                       int indexSize,
                       int compactionThreshold,
                       int compactionThreads,
                       FlushMode flushMode,
                       BufferPool recordPool) throws IOException {

        FileUtils.createDir(root);
        log = new Log<>(root, maxEntrySize, indexSize, compactionThreshold, compactionThreads, flushMode, recordPool, SequenceSegment::new);
        keyPool = BufferPool.localCachePool(256, Long.BYTES, false);
        keyWriteBuffer = keyPool.allocate();
        this.recordWriteBuffer = recordPool.allocate();
    }

    public long append(ByteBuffer data) {
        try {
            long seq = sequence.getAndIncrement();
            keyWriteBuffer.putLong(seq).flip();
            recordWriteBuffer.clear();
            Record.create(keyWriteBuffer, data, recordWriteBuffer);
            recordWriteBuffer.flip();
            keyWriteBuffer.clear();
            log.append(recordWriteBuffer);
            return seq;
        } catch (Exception e) {
            sequence.decrementAndGet();
            throw new RuntimeIOException(e);
        }
    }

    public int find(long sequence, ByteBuffer dst, IndexFunctions fn) {
        //TODO to apply all IndexFunctions, findSegment must also follow the same strategy
        if (sequence < 0) {
            return 0;
        }
        SequenceSegment segment = findSegment(sequence, IndexFunctions.FLOOR);
        if (segment == null) {
            return 0;
        }
        ByteBuffer buffer = keyPool.allocate().putLong(sequence).flip();
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
        keyPool.free(keyWriteBuffer);
        log.close();
    }

    public void delete() {
        log.delete();
    }

    private class SequenceSegment extends IndexedSegment {

        public SequenceSegment(File file, int indexSize) {
            super(file, indexSize, KeyComparator.LONG);
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
