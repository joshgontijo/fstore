package io.joshworks.ilog.lsm;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.ilog.FlushMode;
import io.joshworks.ilog.IndexedSegment;
import io.joshworks.ilog.Log;
import io.joshworks.ilog.Record;
import io.joshworks.ilog.Record2;
import io.joshworks.ilog.index.KeyComparator;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class SequenceLog implements Closeable {

    private final Log<SequenceSegment> log;
    private final BufferPool keyPool;
    private final AtomicLong sequence = new AtomicLong();
    private final ByteBuffer keyWriteBuffer;
    private final ByteBuffer recordWriteBuffer;

    public SequenceLog(File root, int maxEntrySize, int indexSize, int compactionThreshold, FlushMode flushMode, BufferPool recordPool) throws IOException {
        log = new Log<>(root, maxEntrySize, indexSize, compactionThreshold, flushMode, recordPool, SequenceSegment::new);
        keyPool = BufferPool.localCachePool(256, Long.BYTES, false);
        keyWriteBuffer = keyPool.allocate();
        this.recordWriteBuffer = recordPool.allocate();
    }

    public void append(ByteBuffer data) {
        try {
            long seq = sequence.getAndIncrement();
            data.clear();
            keyWriteBuffer.putLong(seq).flip();
            recordWriteBuffer.clear();
            Record2.create(keyWriteBuffer, data, recordWriteBuffer);
            recordWriteBuffer.flip();
            keyWriteBuffer.clear();
            log.append(recordWriteBuffer);
        } catch (Exception e) {
            sequence.decrementAndGet();
            throw new RuntimeIOException(e);
        }
    }

    public void get(long sequence, ByteBuffer dst) {
        SequenceSegment segment = findSegment(sequence);
        ByteBuffer buffer = keyPool.allocate().putLong(sequence).flip();
        try {
            long pos = segment.find(buffer);
            if (pos < 0) {
                return;
            }
            segment.read(pos, dst);
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        } finally {
            keyPool.free(buffer);
        }
    }

    private SequenceSegment findSegment(long sequence) {
        return log.apply(segs -> findSegment(segs, sequence));
    }

    private static SequenceSegment findSegment(List<SequenceSegment> segments, long sequence) {
        int idx = indexedBinarySearch(segments, sequence, SequenceSegment::firstKey, Long::compare);
        idx = idx < 0 ? Math.abs(idx) - 2 : idx;
        return segments.get(idx);
    }

    private static <T, R> int indexedBinarySearch(List<? extends T> segments, R key, Function<T, R> mapper, Comparator<? super R> c) {
        int low = 0;
        int high = segments.size() - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            T midVal = segments.get(mid);
            R mapped = mapper.apply(midVal);
            int cmp = c.compare(mapped, key);

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

    private static Record create(long key, String value) {
        return Record.create(key, Serializers.LONG, value, Serializers.STRING, ByteBuffer.allocate(64));
    }

    @Override
    public void close() {
        keyPool.free(keyWriteBuffer);
        log.close();
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
                return keyBuffer.getLong();
            } finally {
                keyPool.free(keyBuffer);
            }
        }
    }
}
