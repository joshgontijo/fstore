package io.joshworks.es2;

import io.joshworks.fstore.core.util.ByteBufferChecksum;

import java.nio.ByteBuffer;

/**
 * <pre>
 *
 * RECORD_SIZE (4 BYTES)
 * STREAM_HASH (8 BYTES)
 * START_VERSION (4 BYTES)
 * ENTRIES (4 BYTES)
 * CHECKSUM (4 BYTES)
 * CODEC (1 BYTES)
 *
 * DATA [{@link Event}] (N BYTES)
 *
 * </pre>
 */
public class StreamBlock {

    public static final int HEADER_BYTES =
            Integer.BYTES +  //RECORD_SIZE
                    Long.BYTES + //STREAM_HASH
                    Integer.BYTES + //START_VERSION
                    Integer.BYTES + // ENTRIES
                    Integer.BYTES +// CHECKSUM
                    Byte.BYTES;// CHECKSUM


    static final int SIZE_OFFSET = 0;
    static final int STREAM_OFFSET = SIZE_OFFSET + Integer.BYTES;
    static final int START_VERSION_OFFSET = STREAM_OFFSET + Long.BYTES;
    static final int ENTRIES_OFFSET = START_VERSION_OFFSET + Integer.BYTES;
    static final int CHECKSUM_OFFSET = ENTRIES_OFFSET + Integer.BYTES;
    static final int CODEC_OFFSET = CHECKSUM_OFFSET + Integer.BYTES;


    public static int sizeOf(ByteBuffer rec) {
        return rec.getInt(rec.position() + SIZE_OFFSET);
    }

    public static long stream(ByteBuffer rec) {
        return rec.getLong(rec.position() + STREAM_OFFSET);
    }

    public static int startVersion(ByteBuffer rec) {
        return rec.getInt(rec.position() + START_VERSION_OFFSET);
    }

    public static int entries(ByteBuffer rec) {
        return rec.getInt(rec.position() + ENTRIES_OFFSET);
    }

    public static int checksum(ByteBuffer rec) {
        return rec.getInt(rec.position() + CHECKSUM_OFFSET);
    }

    public static int codec(ByteBuffer rec) {
        return rec.getInt(rec.position() + CODEC_OFFSET);
    }

    public static boolean isValid(ByteBuffer chunk) {
        int recSize = sizeOf(chunk);
        if (chunk.remaining() < HEADER_BYTES || chunk.remaining() < recSize) {
            return false;
        }
        int checksum = checksum(chunk);
        int computed = computeChecksum(chunk, recSize - HEADER_BYTES);
        return computed == checksum;
    }

    //expects compressed data already present,starting at position HEADER_BYTES, fills header fields,
    //limit must the end of the compressed data.
    //position must be zero
    public static void writeHeader(ByteBuffer chunkData, long stream, int entries, int startVersion, byte codec) {
        assert chunkData.position() == 0;

        chunkData.putInt(SIZE_OFFSET, chunkData.remaining()); //RECORD_SIZE (HEADER + compressed data)
        chunkData.putLong(STREAM_OFFSET, stream); //STREAM_HASH
        chunkData.putInt(START_VERSION_OFFSET, startVersion); //START_VERSION
        chunkData.putInt(ENTRIES_OFFSET, entries); //ENTRIES

        int checksum = computeChecksum(chunkData, chunkData.remaining());
        chunkData.putInt(CHECKSUM_OFFSET, checksum);
        chunkData.put(CODEC_OFFSET, codec);

    }

    private static int computeChecksum(ByteBuffer chunkData, int recSize) {
        return ByteBufferChecksum.crc32(chunkData, HEADER_BYTES, recSize - HEADER_BYTES);
    }

    public static String toString(ByteBuffer data) {
        return "RECORD_SIZE=" + sizeOf(data) + ", " +
                "STREAM_HASH=" + stream(data) + ", " +
                "START_VERSION=" + startVersion(data) + ", " +
                "ENTRIES=" + entries(data) + ", " +
                "CHECKSUM=" + checksum(data) + ", " +
                "CODEC=" + codec(data);
    }

}
