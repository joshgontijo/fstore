package io.joshworks.ilog;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.ilog.index.Index;
import io.joshworks.ilog.index.IndexFunction;
import io.joshworks.ilog.index.RowKey;
import io.joshworks.ilog.record.Record;
import io.joshworks.ilog.record.RecordPool;
import io.joshworks.ilog.record.Records;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;

import static io.joshworks.ilog.index.Index.NONE;

public class IndexedSegment extends Segment {

    private static final Logger log = LoggerFactory.getLogger(IndexedSegment.class);

    private final RowKey rowKey;
    protected Index index;

    public IndexedSegment(File file, RecordPool pool, RowKey rowKey, long indexEntries) {
        super(file, pool, NO_MAX_SIZE);
        this.rowKey = rowKey;
        this.index = openIndex(file, indexEntries, rowKey);
    }

    protected Index openIndex(File file, long indexEntries, RowKey comparator) {
        File indexFile = LogUtil.indexFile(file);
        return new Index(indexFile, indexEntries, comparator);
    }

    @Override
    public synchronized void restore() {
        int indexCapacity = index.capacity();
        index.delete();
        this.index = openIndex(file, indexCapacity, rowKey);
        super.restore();
    }

    @Override
    protected void onRecordRestored(Record record, long recPos) {
        index.write(record, recPos);
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

    @Override
    protected long append(Records records, int offset, int count) {
        if (index.isFull()) {
            throw new IllegalStateException("Index is full");
        }
        long recordPos = super.append(records, offset, count);
        ;
        try {
            for (int i = 0; i < count; i++) {
                Record rec = records.get(offset + i);
                index.write(rec, recordPos);
                recordPos += rec.recordSize();
            }
            return count;

        } catch (Exception e) {
            throw new RuntimeIOException("Failed to write to segment", e);
        }
    }

    @Override
    public int append(Records records, int offset) {
        int count = Math.min(index.remaining(), records.size() - offset);
        append(records, offset, count);
        return count;
    }

    @Override
    void forceRoll() {
        super.forceRoll();
        index.complete();
    }

    @Override
    public void flush() {
        super.flush();
        index.flush();
    }

    @Override
    public boolean isFull() {
        return index.isFull();
    }

    @Override
    protected void doDelete() {
        log.info("Deleting {}", name());
        channel.delete();
        index.delete();
    }

    @Override
    public void close() {
        super.close();
        index.close();
    }

}
