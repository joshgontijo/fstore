package io.joshworks.es;

import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.ByteBufferChecksum;

import java.nio.ByteBuffer;

/**
 * <pre>
 * RECORD_SIZE (4 BYTES)
 * CHECKSUM (4 BYTES)
 * SEQUENCE (8 BYTES)
 * TIMESTAMP (8 BYTES)
 * STREAM (8 BYTES)
 * VERSION (4 BYTES)
 * ATTRIBUTES (2 BYTES)
 * DATA (N BYTES)
 * </pre>
 */
public class Event {

    public static final int HEADER_BYTES =
            Integer.BYTES +  //RECORD_SIZE
                    Integer.BYTES +  //CHECKSUM
                    Long.BYTES + // SEQUENCE
                    Long.BYTES + // TIMESTAMP
                    Long.BYTES + //STREAM
                    Integer.BYTES + //VERSION
                    Short.BYTES; //ATTRIBUTES

    private Event() {

    }

    private static int SIZE_OFFSET = 0;
    private static int CHECKSUM_OFFSET = SIZE_OFFSET + Integer.BYTES;
    private static int SEQUENCE_OFFSET = CHECKSUM_OFFSET + Integer.BYTES;
    private static int TIMESTAMP_OFFSET = SEQUENCE_OFFSET + Long.BYTES;
    private static int STREAM_OFFSET = TIMESTAMP_OFFSET + Long.BYTES;
    private static int VERSION_OFFSET = STREAM_OFFSET + Long.BYTES;
    private static int ATTRIBUTES_OFFSET = VERSION_OFFSET + Integer.BYTES;
    private static int DATA_OFFSET = ATTRIBUTES_OFFSET + Short.BYTES;

    public static int sizeOf(ByteBuffer data) {
        return sizeOf(data, data.position());
    }

    public static long stream(ByteBuffer data) {
        return data.getLong(data.position() + STREAM_OFFSET);
    }

    public static long version(ByteBuffer data) {
        return data.getInt(data.position() + VERSION_OFFSET);
    }

    public static long sequence(ByteBuffer data) {
        return data.getLong(data.position() + SEQUENCE_OFFSET);
    }

    public static long timestamp(ByteBuffer data) {
        return data.getLong(data.position() + TIMESTAMP_OFFSET);
    }

    public static int checksum(ByteBuffer data) {
        return data.getInt(data.position() + CHECKSUM_OFFSET);
    }

    public static boolean hasAttribute(ByteBuffer data, int attribute) {
        short attr = attributes(data);
        return (attr & (1 << attribute)) == 1;
    }

    public static short attributes(ByteBuffer data) {
        return data.getShort(data.position() + ATTRIBUTES_OFFSET);
    }

    private static short attribute(int... attributes) {
        short b = 0;
        for (int attr : attributes) {
            b = (short) (b | 1 << attr);
        }
        return b;
    }


    public static ByteBuffer create(long sequence, long stream, int version, ByteBuffer data, int... attr) {
        int recSize = HEADER_BYTES + data.remaining();
        ByteBuffer dst = Buffers.allocate(recSize, false);
        dst.putInt(recSize);
        dst.putInt(0); //tmp checksum
        dst.putLong(sequence);
        dst.putLong(System.currentTimeMillis());
        dst.putLong(stream);
        dst.putInt(version);
        dst.putShort(attribute(attr));
        Buffers.copy(data, dst);

        //from
        int checksumStart = Integer.BYTES * 2;
        int checksum = ByteBufferChecksum.crc32(dst, checksumStart, recSize - checksumStart);
        dst.putInt(Integer.BYTES, checksum);

        dst.flip();
        assert dst.remaining() == recSize;

        return dst;
    }

    public static boolean isValid(ByteBuffer data) {
        return isValid(data, data.position());
    }

    static boolean isValid(ByteBuffer recData, int offset) {
        if (!hasHeaderData(recData, offset)) {
            return false;
        }

        int recSize = sizeOf(recData, offset);
        if (recSize <= 0 || recSize > Buffers.remaining(recData, offset)) {
            return false;
        }

        int chksOffset = offset + SEQUENCE_OFFSET; //from TIMESTAMP
        int chksLen = recSize - (Integer.BYTES * 2);

        int checksum = recData.getInt(offset + CHECKSUM_OFFSET);
        int computed = ByteBufferChecksum.crc32(recData, chksOffset, chksLen);

        return computed == checksum;
    }

    private static boolean hasHeaderData(ByteBuffer recData, int offset) {
        return Buffers.remaining(recData, offset) >= HEADER_BYTES;
    }

    private static int sizeOf(ByteBuffer data, int offset) {
        return data.getInt(offset + SIZE_OFFSET);
    }

    public static String toString(long stream, int version) {
        return stream + "@" + version;
    }

    public static String toString(ByteBuffer data) {
        if (!isValid(data)) {
            throw new IllegalArgumentException("Invalid event data");
        }
        return stream(data) + "@" + version(data) + " [" +
                "size=" + sizeOf(data) + ", " +
                "sequence=" + sequence(data) + ", " +
                "timestamp=" + timestamp(data) + ", " +
                "checksum=" + checksum(data) + ", " +
                "attributes=" + attributes(data) +
                "]";
    }

}
