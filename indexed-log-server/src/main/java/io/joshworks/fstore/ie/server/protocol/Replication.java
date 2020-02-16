package io.joshworks.fstore.ie.server.protocol;

import java.nio.ByteBuffer;

public class Replication {

    public static final int TYPE_LAST_REPLICATED = 1;

    public static void replicated(ByteBuffer dst, long id) {
        dst.putInt(TYPE_LAST_REPLICATED);
        dst.putLong(id);
    }

    public static int messageType(ByteBuffer src) {
        return src.getInt(src.position());
    }

    public static long lastReplicatedId(ByteBuffer src) {
        return src.getLong(src.position() + Integer.BYTES);
    }


}
