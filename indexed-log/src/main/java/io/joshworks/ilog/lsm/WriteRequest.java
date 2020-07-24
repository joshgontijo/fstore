package io.joshworks.ilog.lsm;

import io.joshworks.fstore.core.io.buffers.Buffers;

import java.nio.ByteBuffer;

public class WriteRequest {

    private static final ByteBuffer EMPTY = Buffers.allocate(0, false);

    private ByteBuffer key;
    private ByteBuffer value;
    private short attributes;

    private WriteRequest(ByteBuffer key, ByteBuffer value, short attributes) {
        this.key = key;
        this.value = value;
        this.attributes = attributes;
    }

    public static WriteRequest create(ByteBuffer key, ByteBuffer value) {
        return new WriteRequest(key, value, (short) 0);
    }

    public static WriteRequest delete(ByteBuffer key) {
        return new WriteRequest(key, EMPTY, RecordFlags.addDeletionFlag((short) 0));
    }

}
