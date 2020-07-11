package io.joshworks.ilog.record;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Queue;

public class RecordPool {

    //object cache
    private final Queue<Records> bufferRecordsCache = new ArrayDeque<>();

    private final StripedBufferPool pool;
    private final int batchSize;

    RecordPool(StripedBufferPool pool, int batchSize) {
        this.pool = pool;
        this.batchSize = batchSize;
    }

    public static PoolConfig create() {
        return new PoolConfig();
    }

    public Records empty() {
        return allocateRecords();
    }

    public Records fromBuffer(ByteBuffer data, int offset, int count) {
        Records records = allocateRecords();
        records.add(data);
        return records;
    }


    public Records fromBuffer(ByteBuffer data) {
        Records records = allocateRecords();
        records.add(data);
        return records;
    }

    Records allocateRecords() {
        Records records = bufferRecordsCache.poll();
        if (records == null) {
            return new Records(this, batchSize);
        }
        return records;
    }

    public ByteBuffer allocate(int size) {
        return pool.allocate(size);
    }

    public void free(ByteBuffer buffer) {
        pool.free(buffer);
    }

    void free(Records records) {
        if (records == null) {
            return;
        }
        bufferRecordsCache.offer(records);
    }

}
