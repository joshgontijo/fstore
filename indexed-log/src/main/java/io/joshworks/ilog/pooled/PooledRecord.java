package io.joshworks.ilog.pooled;

import io.joshworks.fstore.core.io.buffers.Buffers;

import java.nio.ByteBuffer;

/**
 * VALUE_LEN (4 BYTES)
 * CHECKSUM (4 BYTES)
 * TIMESTAMP (8 BYTES)
 * ATTR (1 BYTES)
 * KEY_LEN (4 BYTES)
 * <p>
 * [KEY] (N BYTES)
 * [VALUE] (N BYTES)
 */
public class PooledRecord extends Pooled {

    public static final int HEADER_SIZE = Integer.BYTES + Integer.BYTES + Long.BYTES + Byte.BYTES + Integer.BYTES;

    int valueLen;
    int checksum;
    long timestamp;
    byte attributes;
    int keyLen;
    ByteBuffer key;
    ByteBuffer value;

    PooledRecord(ObjectPool.Pool<PooledRecord> pool, int keySize, int maxValue) {
        super(pool, keySize + HEADER_SIZE + keySize + maxValue, false);
        this.key = Buffers.allocate(keySize, false);
        this.value = Buffers.allocate(maxValue, false);
    }

    public ByteBuffer buffer() {
        return data;
    }

    public void read() {
        valueLen = data.getInt();
        checksum = data.getInt();
        timestamp = data.getLong();
        attributes = data.get();
        keyLen = data.getInt();
        Buffers.copy(data, data.position(), keyLen, key);
        Buffers.copy(data, data.position(), valueLen, value);
    }


    @Override
    public void close() {

    }
}
