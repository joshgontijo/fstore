package io.joshworks.ilog.pooled;

import io.joshworks.fstore.core.io.buffers.Buffers;

import java.nio.ByteBuffer;

import static io.joshworks.fstore.core.io.buffers.Buffers.relativePosition;

/**
 * VALUE_LEN (4 BYTES)
 * TIMESTAMP (8 BYTES)
 * ATTR (1 BYTES)
 * <p>
 * [VALUE] (N BYTES)
 */
public class BlockRecord extends Pooled {

    private static final int HEADER_SIZE = Integer.BYTES + Long.BYTES + Byte.BYTES;

    final ByteBuffer decompressed;

    BlockRecord(ObjectPool.Pool<? extends Pooled> pool, int size, boolean direct) {
        super(pool, size, direct);
        this.decompressed = Buffers.allocate(size, direct);
    }

    public ByteBuffer buffer() {
        return data;
    }

    public int valueSize() {
        return data.get(relativePosition(data, 0));
    }

}
