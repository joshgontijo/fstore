package io.joshworks.es;

import java.nio.ByteBuffer;

public class Event {

    public int size;
    public long stream;
    public int version;
    public long sequence;
    public short attributes;
    public byte[] data;

    public static long stream(ByteBuffer data) {
        return -1;
    }

    public static ByteBuffer create(long sequence, long stream, int version, ByteBuffer data) {
        ByteBuffer dst = ByteBuffer.allocate(4096);

        return dst;
    }

}
