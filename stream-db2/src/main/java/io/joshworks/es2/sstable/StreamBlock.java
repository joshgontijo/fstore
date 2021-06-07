package io.joshworks.es2.sstable;

import io.joshworks.es2.Event;
import io.joshworks.es2.index.IndexKey;
import io.joshworks.es2.sstable.BlockCodec;
import io.joshworks.fstore.core.codec.Codec;
import io.joshworks.fstore.core.io.buffers.Buffers;
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
 * UNCOMPRESSED_SIZE (4 BYTES)
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
                    Integer.BYTES +// UNCOMPRESSED_SIZE
                    Byte.BYTES;// CHECKSUM


    static final int SIZE_OFFSET = 0;
    static final int STREAM_OFFSET = SIZE_OFFSET + Integer.BYTES;
    static final int START_VERSION_OFFSET = STREAM_OFFSET + Long.BYTES;
    static final int ENTRIES_OFFSET = START_VERSION_OFFSET + Integer.BYTES;
    static final int CHECKSUM_OFFSET = ENTRIES_OFFSET + Integer.BYTES;
    static final int UNCOMPRESSED_SIZE_OFFSET = CHECKSUM_OFFSET + Integer.BYTES;
    static final int CODEC_OFFSET = UNCOMPRESSED_SIZE_OFFSET + Integer.BYTES;


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

    public static int uncompressedSize(ByteBuffer rec) {
        return rec.getInt(rec.position() + UNCOMPRESSED_SIZE_OFFSET);
    }

    public static int checksum(ByteBuffer rec) {
        return rec.getInt(rec.position() + CHECKSUM_OFFSET);
    }

    public static byte codec(ByteBuffer rec) {
        return rec.get(rec.position() + CODEC_OFFSET);
    }

    public static boolean isValid(ByteBuffer chunk) {
        int recSize = sizeOf(chunk);
        if (chunk.remaining() < HEADER_BYTES || chunk.remaining() < recSize) {
            return false;
        }
        int checksum = checksum(chunk);
        int computed = computeChecksum(chunk, recSize);
        return computed == checksum;
    }

    public static int compare(ByteBuffer blockA, ByteBuffer blockB) {
        var streamA = stream(blockA);
        var versionA = startVersion(blockA);

        var streamB = stream(blockB);
        var versionB = startVersion(blockB);

        return IndexKey.compare(streamA, versionA, streamB, versionB);
    }

    //expects compressed data already present,starting at position HEADER_BYTES, fills header fields,
    //limit must the end of the compressed data.
    //position must be zero
    public static void writeHeader(ByteBuffer chunkData, long stream, int startVersion, int entries, int uncompressedSize, BlockCodec codec) {
        assert chunkData.position() == 0;

        chunkData.putInt(SIZE_OFFSET, chunkData.remaining()); //RECORD_SIZE (HEADER + compressed data)
        chunkData.putLong(STREAM_OFFSET, stream); //STREAM_HASH
        chunkData.putInt(START_VERSION_OFFSET, startVersion); //START_VERSION
        chunkData.putInt(ENTRIES_OFFSET, entries); //ENTRIES
        chunkData.putInt(CHECKSUM_OFFSET, computeChecksum(chunkData, chunkData.remaining())); //CHECKSUM
        chunkData.putInt(UNCOMPRESSED_SIZE_OFFSET, uncompressedSize); //UNCOMPRESSED_SIZE
        chunkData.put(CODEC_OFFSET, codec.id); //CODEC
    }

    private static int computeChecksum(ByteBuffer chunkData, int recSize) {
        return ByteBufferChecksum.crc32(chunkData, HEADER_BYTES, recSize - HEADER_BYTES);
    }

    public static int decompress(ByteBuffer chunkData, ByteBuffer dst) {
        int uncompressedSize = uncompressedSize(chunkData);
        if (dst.remaining() < uncompressedSize) {
            throw new RuntimeException("Unable to decompress block: Not enough dst buffer data");
        }
        Buffers.offsetLimit(dst, uncompressedSize);
        var codec = BlockCodec.from(codec(chunkData));
        codec.decompress(chunkData.slice(HEADER_BYTES, chunkData.remaining() - HEADER_BYTES), dst);
        return uncompressedSize;
    }

    public static String toString(ByteBuffer data) {
        return "RECORD_SIZE=" + sizeOf(data) + ", " +
                "STREAM_HASH=" + stream(data) + ", " +
                "START_VERSION=" + startVersion(data) + ", " +
                "ENTRIES=" + entries(data) + ", " +
                "CHECKSUM=" + checksum(data) + ", " +
                "UNCOMPRESSED_SIZE=" + uncompressedSize(data) + ", " +
                "CODEC=" + codec(data);
    }

}
