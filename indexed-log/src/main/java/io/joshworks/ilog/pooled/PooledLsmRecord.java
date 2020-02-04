package io.joshworks.ilog.pooled;

import static io.joshworks.fstore.core.io.buffers.Buffers.relativePosition;

/**
 * KEY_LEN (4 bytes)
 * VAL_LEN (4 BYTES)
 * TIMESTAMP (8 bytes)
 * ATTRIBUTE (1 byte)
 * KEY (N bytes)
 * VAL (N Bytes)
 */
public class PooledLsmRecord extends Pooled {

    public static final int DELETION_ATTR = 0;
    private static final int HAS_MAX_AGE = 1 << 1;

    private static final int KEY_LEN_LEN = Integer.BYTES;
    private static final int VAL_LEN_LEN = Integer.BYTES;
    private static final int TIMESTAMP_LEN_LEN = Integer.BYTES;
    private static final int ATTRIBUTE_LEN = Byte.BYTES;

    private static final int KEY_LEN_OFFSET = 0;
    private static final int VAL_LEN_OFFSET = KEY_LEN_OFFSET + KEY_LEN_LEN;
    private static final int TIMESTAMP_OFFSET = VAL_LEN_OFFSET + VAL_LEN_LEN;
    private static final int ATTRIBUTE_OFFSET = TIMESTAMP_OFFSET + TIMESTAMP_LEN_LEN;
    private static final int KEY_OFFSET = ATTRIBUTE_OFFSET + ATTRIBUTE_LEN;

    PooledLsmRecord(ObjectPool.Pool<? extends Pooled> pool, int size, boolean direct) {
        super(pool, size, direct);
    }

    public int keySize() {
        return data.getInt(relativePosition(data, KEY_LEN_OFFSET));
    }

    public long timestamp() {
        return data.getLong(relativePosition(data, TIMESTAMP_OFFSET));
    }

    public void copyKey(Key key) {
        key.copyFrom(this, relativePosition(data, KEY_OFFSET), keySize());
        key.data.flip();
    }

    public byte attributes() {
        return data.get(relativePosition(data, ATTRIBUTE_OFFSET));
    }

    public boolean expired(long maxAgeSeconds) {
        boolean hasMaxAge = hasAttribute(attributes(), HAS_MAX_AGE);
        if (!hasMaxAge) {
            return false;
        }
        long timestamp = timestamp();
        long now = nowSeconds();
        return maxAgeSeconds > 0 && (now - timestamp > maxAgeSeconds);
    }

    private static boolean hasAttribute(byte attr, int shift) {
        return (attr & (1 << shift)) == 1;
    }

    private static long nowSeconds() {
        return System.currentTimeMillis() / 1000;
    }


}
