package io.joshworks.ilog.lsm;

import io.joshworks.ilog.Record;

import java.nio.ByteBuffer;

public class RecordFlags {

    public static final int DELETION_ATTR = 0;
    private static final int HAS_MAX_AGE = 1 << 1;

    public static boolean deletion(ByteBuffer record) {
        return Record.hasAttribute(record, DELETION_ATTR);
    }

    public static boolean expired(ByteBuffer record, long maxAgeSeconds) {
        boolean hasMaxAge = Record.hasAttribute(record, HAS_MAX_AGE);
        if (!hasMaxAge) {
            return false;
        }
        long timestamp = Record.TIMESTAMP.get(record);
        long now = nowSeconds();
        return maxAgeSeconds > 0 && (now - timestamp > maxAgeSeconds);
    }

    private static long nowSeconds() {
        return System.currentTimeMillis() / 1000;
    }


}
