package io.joshworks.fstore.tcp.codec;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;

/**
 * UNCOMPRESSED_LEN (4bytes)
 * COMPRESSION (1 bytes)
 */
public class TcpHeader {

    public static final int BYTES = Integer.BYTES + Byte.BYTES;

    private static final Compression[] compression = Arrays.stream(Compression.values()).sorted(Comparator.comparingInt(Compression::val)).toArray(Compression[]::new);

    public static int uncompressedLength(ByteBuffer buffer) {
        return buffer.getInt(buffer.position());
    }

    public static void uncompressedLength(ByteBuffer buffer, int len) {
        buffer.putInt(buffer.position(), len);
    }

    public static float compressionRatio(ByteBuffer buffer) {
        float uncompressed = uncompressedLength(buffer);
        return (uncompressed - buffer.remaining()) / 100;
    }

    public static void compression(ByteBuffer buffer, Compression compression) {
        buffer.put(buffer.position() + Integer.BYTES, (byte) compression.val());
    }

    public static Compression compression(ByteBuffer buffer) {
        return compression[buffer.get(buffer.position() + Integer.BYTES)];
    }

}
