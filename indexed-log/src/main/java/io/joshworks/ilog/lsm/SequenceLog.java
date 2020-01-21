package io.joshworks.ilog.lsm;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.core.util.TestUtils;
import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.ilog.FlushMode;
import io.joshworks.ilog.IndexedSegment;
import io.joshworks.ilog.Log;
import io.joshworks.ilog.Record;
import io.joshworks.ilog.index.Index;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class SequenceLog {

    private final Log log;
    private final BufferPool keyPool;
    private final AtomicLong sequence = new AtomicLong();

    public SequenceLog(File root, int maxEntrySize, int indexSize, int compactionThreshold, FlushMode flushMode, BufferPool pool) throws IOException {
        log = new Log(root, maxEntrySize, indexSize, compactionThreshold, flushMode, pool, IndexedSegment::new, Index.LONG);
        keyPool = BufferPool.localCachePool(256, Long.BYTES, false);
    }

    public void append(String data) {
        try {
            long seq = sequence.getAndIncrement();
            var buffer = ByteBuffer.allocate(4096);
            Record record = Record.create(seq, Serializers.LONG, data, Serializers.STRING, buffer);
            log.append(record);
        } catch (Exception e) {
            sequence.decrementAndGet();
            throw new RuntimeIOException(e);
        }
    }

    public void get(long sequence, ByteBuffer dst) {
        IndexedSegment segment = findSegment(sequence);
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

    private IndexedSegment findSegment(long sequence) {
        return log.apply(segs -> findSegment(segs, sequence));
    }

    private static IndexedSegment findSegment(List<IndexedSegment> segments, long sequence) {
        int idx = indexedBinarySearch(segments, sequence, IndexedSegment::segmentId, Long::compare);
        return segments.get(idx);
    }

    private static <T, R> int indexedBinarySearch(List<? extends T> l, R key, Function<T, R> mapper, Comparator<? super R> c) {
        int low = 0;
        int high = l.size() - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            T midVal = l.get(mid);
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

    public static void main(String[] args) throws IOException {

        BufferPool bufferPool = BufferPool.localCachePool(256, 1024, false);
        SequenceLog log = new SequenceLog(TestUtils.testFolder(), 1024, Size.MB.ofInt(5), 2, FlushMode.ON_ROLL, bufferPool);
        for (int i = 0; i < 1000000; i++) {
            log.append(String.valueOf(i));
        }

        for (int i = 0; i < 1000000; i++) {
            var rbuff = ByteBuffer.allocate(1024);
            log.get(i, rbuff);
            rbuff.flip();

            if (!rbuff.hasRemaining()) {
                System.err.println("No data for " + i);
            }

            Record record;
            while ((record = Record.from(rbuff, false)) != null) {
                String toString = record.toString(Serializers.LONG, Serializers.STRING);
                System.out.println(toString);
            }

        }

    }

    private static Record create(long key, String value) {
        return Record.create(key, Serializers.LONG, value, Serializers.STRING, ByteBuffer.allocate(64));
    }


}
