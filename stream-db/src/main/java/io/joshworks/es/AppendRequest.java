package io.joshworks.es;

import java.nio.ByteBuffer;

public class AppendRequest {

    public long stream;
    public int expectedVersion;
    public byte[] data;

    public static boolean hasNext(ByteBuffer events) {
        return false;
    }

    public static long stream(ByteBuffer events) {
        return 0;
    }

    public static int expectedVersion(ByteBuffer events) {
        return 0;
    }
}
