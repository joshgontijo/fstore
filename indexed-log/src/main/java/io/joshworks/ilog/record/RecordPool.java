package io.joshworks.ilog.record;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.ObjectPool;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

public class RecordPool {

    //object cache
    private final ObjectPool<Records> cache;
    private final ObjectPool<Record> recordCache;

    private final StripedBufferPool pool;

    RecordPool(StripedBufferPool pool, int batchSize) {
        this.pool = pool;
        this.cache = new ObjectPool<>(p -> new Records(this, batchSize));
        this.recordCache = new ObjectPool<>(p -> new Record(this));
    }

    public static PoolConfig create() {
        return new PoolConfig();
    }

    public Records empty() {
        return allocateRecords();
    }

    public Records fromBuffer(ByteBuffer data, int offset, int count) {
        Records records = allocateRecords();
        records.add(data, offset, count);
        return records;
    }

    public Records fromBuffer(ByteBuffer data) {
        Records records = allocateRecords();
        records.add(data);
        return records;
    }

    public Record from(ByteBuffer data, int offset) {
        Record rec = allocateRecord();
        if (Record.isValid(data, offset)) {
            int recSize = Record.recordSize(data, offset);
            ByteBuffer buffer = pool.allocate(recSize);
            try {
                int copied = Buffers.copy(data, offset, recSize, buffer);
                assert copied == recSize;
                rec.init(buffer.flip());
            } catch (Exception e) {
                pool.free(buffer);
                throw e;
            }
        }
        return rec;
    }

    Records allocateRecords() {
        return cache.allocate();
    }

    public Record allocateRecord() {
        return recordCache.allocate();
    }

    public ByteBuffer allocate(int size) {
        return pool.allocate(size);
    }

    public void free(ByteBuffer buffer) {
        pool.free(buffer);
    }

    void free(Record record) {
        recordCache.release(record);
    }

    void free(Records records) {
        cache.release(records);
    }

    public void close() {
        pool.close();
        cache.clear();
        recordCache.clear();
    }

    public Records read(FileChannel channel, long position, int count) {
        try {
            Records records = empty();
            ByteBuffer dst = pool.allocate(count);
            int read = channel.read(dst, position);
            if (read == Storage.EOF) {
                throw new RuntimeIOException("Channel closed");
            }
            dst.flip();

            records.add(dst);

            return records;

        } catch (Exception e) {
            throw new RuntimeIOException("Failed to read entry: " + channel + ", position: " + position + ", count: " + count, e);
        }
    }

    public Record get(FileChannel channel, long position, int count) {
        try {
            ByteBuffer dst = pool.allocate(count);
            int read = channel.read(dst, position);
            if (read == Storage.EOF) {
                throw new RuntimeIOException("Channel closed");
            }
            dst.flip();

            Record record = allocateRecord();
            if (!Record.isValid(dst)) {
                throw new IllegalStateException("Invalid record");
            }
            record.init(dst);
            return record;

        } catch (Exception e) {
            throw new RuntimeIOException("Failed to read entry: " + channel + ", position: " + position + ", count: " + count, e);
        }
    }
}
