package io.joshworks.ilog.lsm;


import io.joshworks.ilog.record.Record;

import java.nio.ByteBuffer;

public class RecordFlags {

    private static final int DELETION_ATTR = 1 << 1;
    private static final int HAS_MAX_AGE = 1 << 2;

    public static short addDeletionFlag(short attributes) {
        return  (short) (attributes | (((short)1) << DELETION_ATTR));
    }

    public static boolean deletion(ByteBuffer data) {
        return deletion(data, data.position());
    }

    public static boolean deletion(ByteBuffer data, int offset) {
        return Record.hasAttribute(data, offset, DELETION_ATTR);
    }

    public static boolean expired(ByteBuffer data, long maxAgeSeconds) {
        return expired(data, data.position(), maxAgeSeconds);
    }

    public static boolean expired(ByteBuffer data, int offset, long maxAgeSeconds) {
        boolean hasMaxAge = Record.hasAttribute(data, offset, HAS_MAX_AGE);
        if (!hasMaxAge) {
            return false;
        }
        long timestamp = Record.timestamp(data, offset);
        long now = System.currentTimeMillis();
        long diffSec = (now - timestamp) / 1000;
        return maxAgeSeconds > 0 && (diffSec > maxAgeSeconds);
    }
}
