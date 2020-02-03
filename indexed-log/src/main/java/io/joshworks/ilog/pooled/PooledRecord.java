package io.joshworks.ilog.pooled;

import java.nio.ByteBuffer;

import static io.joshworks.fstore.core.io.buffers.Buffers.relativePosition;

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

    private static final int DATA_LEN_LEN = Integer.BYTES;
    private static final int CHECKSUM_LEN = Integer.BYTES;
    private static final int TIMESTAMP_LEN = Long.BYTES;
    private static final int ATTR_LEN = Byte.BYTES;
    private static final int KEY_LEN_LEN = Integer.BYTES;


    public static final int DATA_LENGTH_OFFSET = 0;
    public static final int CHECKSUM_OFFSET = DATA_LENGTH_OFFSET + DATA_LEN_LEN;
    public static final int TIMESTAMP_OFFSET = CHECKSUM_OFFSET + CHECKSUM_LEN;
    public static final int ATTR_OFFSET = TIMESTAMP_OFFSET + TIMESTAMP_LEN;
    public static final int KEY_LENGTH_OFFSET = ATTR_OFFSET + ATTR_LEN;
    public static final int KEY_OFFSET = KEY_LENGTH_OFFSET + KEY_LEN_LEN;

    PooledRecord(ObjectPool.Pool<PooledRecord> pool, int keySize, int maxValue) {
        super(pool, keySize + HEADER_SIZE + keySize + maxValue, false);
    }

    public ByteBuffer buffer() {
        return data;
    }

    public int valueSize(ByteBuffer buffer) {
        return buffer.getInt(relativePosition(buffer, DATA_LENGTH_OFFSET));
    }

    public int valueOffset(ByteBuffer buffer) {
        return relativePosition(buffer, KEY_OFFSET) + keySize(buffer);
    }

    public int checksum(ByteBuffer buffer) {
        return buffer.getInt(relativePosition(buffer, CHECKSUM_OFFSET));
    }

    public long timestamp(ByteBuffer buffer) {
        return buffer.getLong(relativePosition(buffer, TIMESTAMP_OFFSET));
    }

    public byte attributes(ByteBuffer buffer) {
        return buffer.get(relativePosition(buffer, ATTR_OFFSET));
    }

    public int keyOffset(ByteBuffer buffer) {
        return relativePosition(buffer, KEY_OFFSET);
    }

    public int keySize(ByteBuffer buffer) {
        return buffer.getInt(relativePosition(buffer, KEY_LENGTH_OFFSET));
    }

    public boolean hasAttribute(ByteBuffer buffer, int attribute) {
        byte attr = buffer.get(relativePosition(buffer, ATTR_OFFSET));
        return (attr & (1 << attribute)) == 1;
    }

    @Override
    public void close() {

    }
}
