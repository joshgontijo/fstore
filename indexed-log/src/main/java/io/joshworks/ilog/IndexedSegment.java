package io.joshworks.ilog;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.ilog.index.Index;
import io.joshworks.ilog.index.IndexFunction;
import io.joshworks.ilog.index.RowKey;
import io.joshworks.ilog.record.Record2;
import io.joshworks.ilog.record.RecordPool;
import io.joshworks.ilog.record.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static io.joshworks.ilog.index.Index.NONE;

public class IndexedSegment {

    private static final Logger log = LoggerFactory.getLogger(IndexedSegment.class);

    protected final File file;
    protected final RecordPool pool;
    private final SegmentChannel channel;
    protected final long id;
    protected Index index;

    private final AtomicBoolean markedForDeletion = new AtomicBoolean();
    private final Set<SegmentIterator> iterators = new HashSet<>();

    public IndexedSegment(File file, long indexEntries, RecordPool pool) {
        this.file = file;
        this.pool = pool;
        this.rowKey = pool.rowKey();
        this.index = openIndex(file, indexEntries, rowKey);
        this.id = LogUtil.segmentId(file.getName());
        this.channel = SegmentChannel.open(file);
        ;
    }

    private Index openIndex(File file, long indexEntries, RowKey comparator) {
        File indexFile = LogUtil.indexFile(file);
        return new Index(indexFile, indexEntries, comparator);
    }

    public synchronized void reindex() {
        log.info("Reindexing {}", index.name());

        int indexCapacity = index.capacity();
        index.delete();
        this.index = openIndex(file, indexCapacity, rowKey);

        long start = System.currentTimeMillis();

        try (Records records = pool.fromSegment(this)) {
            int processed = 0;

            long recordPos = 0;
            while (records.hasNext()) {
                Record2 record = records.next();
                recordPos += record.writeToIndex(index, recordPos);
                processed++;
            }
            log.info("Restored {}: {} entries in {}ms", name(), processed, System.currentTimeMillis() - start);
        }

    }

    public Records get(ByteBuffer key, IndexFunction func) {
        int idx = index.find(key, func);
        Records records = pool.empty();
        if (idx == NONE) {
            return records;
        }
        long pos = index.readPosition(idx);
        int len = index.readEntrySize(idx);

        long read = records.from(channel, pos, len);
        assert read == len;

        return records;
    }

    int read(ByteBuffer dst, long offset) {
        try {
            return channel.read(dst, offset);
        } catch (IOException e) {
            throw new RuntimeIOException("Failed to read from " + name(), e);
        }
    }

    //return the number of written records
    public int write(Records records, int offset) {
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
            int count = Math.min(index.remaining(), records.size() - offset);

            long recordPos = channel.position();
            records.writeTo(channel, offset, count);
            for (int i = offset; i < count; i++) {
                Record2 rec = records.get(i);
                index.write(rec, recordPos);
                recordPos += rec.recordSize();
            }
            return count;

        } catch (Exception e) {
            throw new RuntimeIOException("Failed to write to segment");
        }
    }

    public boolean readOnly() {
        return channel.readOnly();
    }

    public long writePosition() {
        return channel.position();
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
        if (!iterators.isEmpty()) {
            log.info("Segment marked for deletion");
            return;
        }
        doDelete();
    }

    public synchronized SegmentIterator iterator(long pos, int bufferSize) {
        SegmentIterator it = new SegmentIterator(this, pos, bufferSize, pool);
        iterators.add(it);
        return it;
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

    public int entries() {
        return index.entries();
    }

    public long indexSize() {
        return index.size();
    }

    void release(SegmentIterator iterator) {
        iterators.remove(iterator);
        if (markedForDeletion.get()) {
            synchronized (this) {
                doDelete();
            }
        }
    }


}
