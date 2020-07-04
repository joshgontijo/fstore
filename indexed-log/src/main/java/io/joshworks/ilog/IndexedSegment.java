package io.joshworks.ilog;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.ilog.index.Index;
import io.joshworks.ilog.index.IndexFunction;
import io.joshworks.ilog.index.RowKey;
import io.joshworks.ilog.record.Record2;
import io.joshworks.ilog.record.RecordPool;
import io.joshworks.ilog.record.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.joshworks.ilog.index.Index.NONE;

public class IndexedSegment {

    private static final Logger log = LoggerFactory.getLogger(IndexedSegment.class);

    public static int START = 0;

    private final File file;
    private final RowKey rowKey;
    private final RecordPool pool;
    private final SegmentChannel channel;
    private final long id;
    protected Index index;

    private final AtomicBoolean markedForDeletion = new AtomicBoolean();

    private final SegmentLock lock = new SegmentLock();

    public IndexedSegment(File file, int indexSize, RecordPool pool) {
        this.file = file;
        this.pool = pool;
        this.rowKey = pool.rowKey();
        this.index = openIndex(file, indexSize, rowKey);
        this.id = LogUtil.segmentId(file.getName());
        this.channel = SegmentChannel.open(file);
        ;
    }

    private Index openIndex(File file, int indexSize, RowKey comparator) {
        File indexFile = LogUtil.indexFile(file);
        return new Index(indexFile, indexSize, comparator);
    }

    synchronized void reindex() throws IOException {
        log.info("Reindexing {}", index.name());

        int indexCapacity = index.capacity();
        index.delete();
        this.index = openIndex(file, indexCapacity, rowKey);

        long start = System.currentTimeMillis();

        try (Records records = pool.fromSegment(this)) {
            int processed = 0;

            long recordPos = 0;
            while (records.hasNext()) {
                Record2 record = records.poll();
                recordPos += record.writeToIndex(index, recordPos);
                processed++;
            }
            log.info("Restored {}: {} entries in {}ms", name(), processed, System.currentTimeMillis() - start);
        }

    }

    public int find(ByteBuffer key, ByteBuffer dst, IndexFunction func) {
        int idx = index.find(key, func);
        if (idx == NONE) {
            return 0;
        }

        int plim = dst.limit();
        int ppos = dst.position();
        try {
            long pos = index.readPosition(idx);
            int len = index.readEntrySize(idx);

            if (len > dst.remaining()) {
                throw new IllegalStateException("Destination buffer remaining bytes is less than entry size");
            }

            Buffers.offsetLimit(dst, len);

            int read = read(pos, dst);
            if (read != len) {
                throw new IllegalStateException("Expected read of " + len + " actual read: " + read);
            }
            dst.limit(plim);

            return read;
        } catch (Exception e) {
            dst.limit(plim).position(ppos);
            throw new RuntimeIOException(e);
        }
    }

    /**
     * Reads a single entry for the given offset, read is performed with a single IO call
     * with a buffer of size specified by readSize. If the buffer is too small for the entry, then a new one is created and
     */
    public int read(long position, ByteBuffer dst) {
        try {
            long writePos = channel.position();
            if (position >= writePos) {
                return readOnly() ? -1 : 0;
            }

            int count = (int) Math.min(dst.remaining(), writePos - position);
            assert count > 0;
            int plim = dst.limit();
            Buffers.offsetLimit(dst, count);
            int read = channel.read(dst, position);
            dst.limit(plim);
            assert read == count;
            return read;
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    public boolean readOnly() {
        return channel.readOnly();
    }

    public void forceRoll() {
        flush();
        channel.truncate();
        index.complete();
    }

    public void roll() {
        if (!channel.markAsReadOnly()) {
            throw new IllegalStateException("Already read only: " + name());
        }
        forceRoll();
    }

    public long size() {
        try {
            return channel.size();
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    public void flush() {
        try {
            channel.force(false);
            index.flush();
        } catch (IOException e) {
            throw new RuntimeIOException("Failed to flush segment", e);
        }
    }

    public long start() {
        return channel.start();
    }

    public FileChannel channel() {
        return channel;
    }

    public String name() {
        return channel.name();
    }

    public Index index() {
        return index;
    }

    public int level() {
        return LogUtil.levelOf(id);
    }

    public long segmentId() {
        return id;
    }

    public long segmentIdx() {
        return LogUtil.segmentIdx(id);
    }

    public synchronized void delete() {
        if (!markedForDeletion.compareAndSet(false, true)) {
            return;
        }
        if (lock.counter.get() > 0) {
            log.info("Segment marked for deletion");
            return;
        }
        doDelete();
    }

    public synchronized SegmentLock lock() {
        if (markedForDeletion.get()) {
            return null;
        }
        lock.counter.incrementAndGet();
        return lock;
    }

    private void doDelete() {
        log.info("Deleting {}", name());
        channel.delete();
        index.delete();
    }

    public void close() throws IOException {
        channel.close();
        index.close();
    }

    @Override
    public String toString() {
        return "IndexedSegment{" +
                "name=" + name() +
                ", writePosition=" + channel.position() +
                ", size=" + size() +
                ", entries=" + index.entries() +
                ", indexSize=" + index.size() +
                '}';
    }

    public static class SegmentLock implements Closeable {

        private final AtomicInteger counter = new AtomicInteger();

        @Override
        public void close() {
            //make sure close is not called twice
            counter.decrementAndGet();
        }
    }

}
