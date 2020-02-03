package io.joshworks.ilog.pooled;

import io.joshworks.fstore.core.io.buffers.Buffers;

import java.nio.ByteBuffer;

/**
 * VALUE_LEN (4 BYTES)
 * TIMESTAMP (8 BYTES)
 * ATTR (1 BYTES)
 * <p>
 * [VALUE] (N BYTES)
 */
public class BlockRecord extends Pooled {

    private static final int HEADER_SIZE = Integer.BYTES + Long.BYTES + Byte.BYTES;

    int valueLen;
    long timestamp;
    byte attributes;
    final ByteBuffer decompressed;

    BlockRecord(ObjectPool.Pool<? extends Pooled> pool, int size, boolean direct) {
        super(pool, size, direct);
        this.decompressed = Buffers.allocate(size, direct);
    }

    public ByteBuffer buffer() {
        return data;
    }

    public void read() {
        valueLen = data.getInt();
        timestamp = data.getLong();
        attributes = data.get();
        Buffers.copy(data, data.position(), valueLen, decompressed);
    }
}
