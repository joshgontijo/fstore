package io.joshworks.ilog;

import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.ilog.index.Index;
import io.joshworks.ilog.index.IndexFunction;
import io.joshworks.ilog.index.RowKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;

import static io.joshworks.ilog.index.Index.NONE;

public class IndexedSegment extends Segment {

    private static final Logger log = LoggerFactory.getLogger(IndexedSegment.class);

    private final RowKey rowKey;
    protected Index index;

    public IndexedSegment(File file, BufferPool pool, RowKey rowKey, long maxEntries) {
        super(file, pool, NO_MAX_SIZE, NO_MAX_ENTRIES);
        this.rowKey = rowKey;
        this.index = openIndex(file, maxEntries, rowKey);
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
    protected void onRecordRestored(ByteBuffer record, long recPos) {
        index.write(record, recPos);
    }

    public int find(ByteBuffer key, IndexFunction func, ByteBuffer dst) {
        int idx = index.find(key, func);
        if (idx == NONE) {
            return 0;
        }
        long pos = index.readPosition(idx);
        return read(dst, pos);
    }

    @Override
    public long append(ByteBuffer records) {
        return append(records, index.remaining());
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
        super.delete();
        index.delete();
    }

    @Override
    public void close() {
        super.close();
        index.close();
    }

}
