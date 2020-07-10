package io.joshworks.ilog.record;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.ilog.IndexedSegment;
import io.joshworks.ilog.index.Index;
import io.joshworks.ilog.index.IndexFunction;
import io.joshworks.ilog.index.RowKey;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Queue;

import static io.joshworks.ilog.index.Index.NONE;

public class RecordPool {

    //object cache
    private final Queue<Records> bufferRecordsCache = new ArrayDeque<>();

    private final StripedBufferPool pool;
    private final int batchSize;

    RecordPool(StripedBufferPool pool, int batchSize) {
        this.pool = pool;
        this.batchSize = batchSize;
    }

    public static PoolConfig create(RowKey rowKey) {
        return new PoolConfig(rowKey);
    }

    public Records empty(RowKey rowKey) {
        return allocateRecords(rowKey);
    }

    public Records fromBuffer(RowKey rowKey, ByteBuffer data) {
        Records records = allocateRecords();
        records.add(data);
        return records;
    }

    public Records read(IndexedSegment segment, ByteBuffer key, IndexFunction func) {
        Index index = segment.index();
        int idx = index.find(key, func);
        if (idx == NONE) {
            return Records.EMPTY;
        }
        long pos = index.readPosition(idx);
        int len = index.readEntrySize(idx);

        ByteBuffer buffer = allocate(len);
        buffer.limit(len);
        try {
            segment.channel().read(buffer, pos);
            buffer.flip();
            return fromBuffer(buffer);
        } catch (Exception e) {
            throw new RuntimeIOException("Failed to read entry", e);
        } finally {
            free(buffer);
        }
    }

    Records allocateRecords(RowKey rowKey) {
        Records records = bufferRecordsCache.poll();
        if (records == null) {
            return new Records(this, rowKey, batchSize);
        }
        return records;
    }

    ByteBuffer allocate(int size) {
        return pool.allocate(size);
    }

    void free(ByteBuffer buffer) {
        pool.free(buffer);
    }

    void free(Records records) {
        if (records == null) {
            return;
        }
        bufferRecordsCache.offer(records);
    }

}
