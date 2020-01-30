package io.joshworks.ilog.lsm;

import io.joshworks.ilog.Record2;

import java.nio.ByteBuffer;

import static io.joshworks.fstore.core.io.buffers.Buffers.relativePosition;

/**
 * KEY (N bytes)
 * VAL_LEN (4 BYTES)
 * TIMESTAMP (8 bytes)
 * VAL (N Bytes)
 */
public class LsmRecord {

    public static final int DELETION_ATTR = 0;
    private static final int HAS_MAX_AGE = 1 << 1;

    public static boolean deletion(ByteBuffer record) {
        return Record2.hasAttribute(record, DELETION_ATTR);
    }

    public static int valueSize(int keyLen, ByteBuffer record) {
        return record.getInt(relativePosition(record, keyLen));
    }

    public static boolean expired(ByteBuffer record, long maxAgeSeconds) {
        boolean hasMaxAge = Record2.hasAttribute(record, HAS_MAX_AGE);
        if (!hasMaxAge) {
            return false;
        }
        long timestamp = Record2.timestamp(record);
        long now = nowSeconds();
        return maxAgeSeconds > 0 && (now - timestamp > maxAgeSeconds);
    }

    private static long nowSeconds() {
        return System.currentTimeMillis() / 1000;
    }

}
