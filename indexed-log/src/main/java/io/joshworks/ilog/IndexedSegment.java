package io.joshworks.ilog;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.ilog.index.Index;
import io.joshworks.ilog.index.RowKey;
import io.joshworks.ilog.record.Record2;
import io.joshworks.ilog.record.RecordPool;
import io.joshworks.ilog.record.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

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

    synchronized void reindex() {
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

    public long append(Records records) {
        if (index.isFull()) {
            throw new IllegalStateException("Index is full");
        }
        if (readOnly()) {
            throw new IllegalStateException("Segment is read only");
        }
        if (records.size() == 0) {
            return 0;
        }

        try {

            long totalWritten = 0;

            long recordPos = channel.position();
            int inserted = 0;
            while (!index.isFull()) {
                //bulk write to data region
                int count = Math.min(index.remaining(), records.size());

                long written = 0;
                long written = records.writeTo(channel, inserted, count);

                buffers.toArray(tmp);
                records.writeTo(channel)
                long written = channel.write(tmp, 0, count);

                totalWritten += written;

                //files are always available, no need to use removeWrittenEntries
                //poll entries and add to index
                for (int i = 0; i < count; i++) {
                    try (Record2 rec = poll()) {
                        recordPos += rec.writeToIndex(index, recordPos);
                        onInsert.accept(rec);
                    }
                }
            }
            return totalWritten;

        } catch (Exception e) {
            throw new RuntimeIOException("Failed to write to segment");
        }
    }

    private void writeToIndex(Record2 rec, long recordPos, Consumer<Record2> onWrite) {
        rec.writeToIndex(index, recordPos);
        onWrite.accept(rec);
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
