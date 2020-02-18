package io.joshworks.fstore.tcp;

import io.joshworks.fstore.tcp.codec.Compression;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;

/**
 * UNCOMPRESSED_LEN (4bytes)
 * COMPRESSION (1 bytes)
 */
public class TcpHeader {

    public static final int RECORD_LEN_LENGTH = Integer.BYTES;
    public static final int COMPRESSION_LENGTH = Byte.BYTES;

    public static final int BYTES = RECORD_LEN_LENGTH + COMPRESSION_LENGTH;

    private static final int RECORD_LEN_OFFSET = 0;
    private static final int COMPRESSION_OFFSET = RECORD_LEN_OFFSET + RECORD_LEN_LENGTH;

    private static final Compression[] compression = Arrays.stream(Compression.values()).sorted(Comparator.comparingInt(Compression::val)).toArray(Compression[]::new);

    public static void messageLength(int baseOffset, ByteBuffer buffer, int len) {
        buffer.putInt(baseOffset + RECORD_LEN_OFFSET, len);
    }

    public static int messageLength(int baseOffset, ByteBuffer buffer) {
        return buffer.getInt(baseOffset + RECORD_LEN_OFFSET);
    }

//    public static int uncompressedLength(int baseOffset, ByteBuffer buffer) {
//        return buffer.getInt(baseOffset + UNCOMPRESSED_LEN_OFFSET);
//    }
//
//    public static void uncompressedLength(int baseOffset, ByteBuffer buffer, int len) {
//        buffer.putInt(baseOffset + UNCOMPRESSED_LEN_OFFSET, len);
//    }

//    public static float compressionRatio(int basedOffset, ByteBuffer buffer) {
//        float uncompressed = uncompressedLength(basedOffset, buffer);
//        return (uncompressed - buffer.remaining()) / 100;
//    }

    public static void compression(int baseOffset, ByteBuffer buffer, Compression compression) {
        buffer.put(baseOffset + COMPRESSION_OFFSET, (byte) compression.val());
    }

    public static Compression compression(int baseOffset, ByteBuffer buffer) {
        return compression[buffer.get(baseOffset + COMPRESSION_OFFSET)];
    }

    //used in CodecConduit, where the first byte is the compression byte
    public static Compression compression(ByteBuffer buffer) {
        return compression[buffer.get(buffer.position())];
    }

//    public static boolean batch(ByteBuffer buffer) {
//        return buffer.get(buffer.position() + Integer.BYTES + Byte.BYTES) == 1;
//    }
//
//    public static void batch(ByteBuffer buffer, boolean batch) {
//        buffer.put(buffer.position() + Integer.BYTES + Byte.BYTES, (byte) (batch ? 1 : 0));
//    }

}
