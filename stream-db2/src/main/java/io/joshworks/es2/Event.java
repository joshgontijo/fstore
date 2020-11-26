package io.joshworks.es2;

import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.ByteBufferChecksum;

import java.nio.ByteBuffer;

/**
 * <pre>
 *
 * RECORD_SIZE (4 BYTES)
 * STREAM_HASH (8 BYTES)
 * VERSION (4 BYTES)
 * CHECKSUM (4 BYTES)
 * SEQUENCE (8 BYTES)
 * TIMESTAMP (8 BYTES)
 * TYPE_LENGTH (2 BYTES)
 * DATA_LENGTH (4 BYTES)
 *
 * EVENT_TYPE (N BYTES)
 * DATA (N BYTES)
 *
 * </pre>
 */
public class Event {

    public static final int NO_VERSION = -1;

    public static final int HEADER_BYTES =
            Integer.BYTES +  //RECORD_SIZE
                    Long.BYTES + //STREAM_HASH
                    Integer.BYTES + //VERSION
                    Integer.BYTES +  //CHECKSUM
                    Long.BYTES + // SEQUENCE
                    Long.BYTES + // TIMESTAMP
                    Short.BYTES + //TYPE_LENGTH
                    Integer.BYTES; //DATA_LENGTH

    private Event() {

    }

    private static final int SIZE_OFFSET = 0;
    private static final int STREAM_OFFSET = SIZE_OFFSET + Integer.BYTES;
    private static final int VERSION_OFFSET = STREAM_OFFSET + Long.BYTES;
    private static final int CHECKSUM_OFFSET = VERSION_OFFSET + Integer.BYTES;
    private static final int SEQUENCE_OFFSET = CHECKSUM_OFFSET + Integer.BYTES;
    private static final int TIMESTAMP_OFFSET = SEQUENCE_OFFSET + Long.BYTES;
    private static final int EVENT_TYPE_LENGTH_OFFSET = TIMESTAMP_OFFSET + Long.BYTES;
    private static final int DATA_LENGTH_OFFSET = EVENT_TYPE_LENGTH_OFFSET + Short.BYTES;
    private static final int EVENT_TYPE_OFFSET = DATA_LENGTH_OFFSET + Integer.BYTES;


    public static int sizeOf(ByteBuffer data) {
        return data.getInt(data.position() + SIZE_OFFSET);
    }

    public static long stream(ByteBuffer data) {
        return data.getLong(data.position() + STREAM_OFFSET);
    }

    public static int version(ByteBuffer data) {
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

    public static short eventTypeLen(ByteBuffer data) {
        return data.getShort(data.position() + EVENT_TYPE_LENGTH_OFFSET);
    }

    public static int dataLen(ByteBuffer data) {
        return data.getInt(data.position() + DATA_LENGTH_OFFSET);
    }

    public static String eventType(ByteBuffer data) {
        int evTypeLen = eventTypeLen(data);
        int offset = data.position() + EVENT_TYPE_OFFSET;
        return Buffers.toString(data, offset, evTypeLen);
    }

    public static boolean isValid(ByteBuffer recData) {
        if (!canReadRecordSize(recData)) {
            return false;
        }

        int recSize = sizeOf(recData);
        if (recSize <= 0 || recSize > recData.remaining()) {
            return false;
        }

        int checksum = checksum(recData);
        int computed = computeChecksum(recData);

        return computed == checksum;
    }

    private static int computeChecksum(ByteBuffer record) {
        short typeLen = eventTypeLen(record);

        int chksOffset = record.position() + EVENT_TYPE_OFFSET + typeLen;
        int chksLen = dataLen(record);
        return ByteBufferChecksum.crc32(record, chksOffset, chksLen);
    }

    public static void writeChecksum(ByteBuffer data) {
        int checksum = computeChecksum(data);
        data.putInt(data.position() + CHECKSUM_OFFSET, checksum);
    }

    private static boolean canReadRecordSize(ByteBuffer recData) {
        return recData.remaining() >= Integer.BYTES;
    }

    public static void writeSequence(ByteBuffer data, int offset, long sequence) {
        data.putLong(offset + SEQUENCE_OFFSET, sequence);
    }

    public static void writeVersion(ByteBuffer data, int version) {
        data.putInt(data.position() + VERSION_OFFSET, version);
    }

    public static void rewrite(ByteBuffer data, long stream, int version) {
        data.putLong(data.position() + STREAM_OFFSET, stream);
        writeVersion(data, version);
        assert Event.isValid(data);
    }

//    public static int serialize(WriteEvent event, int version, long sequence, ByteBuffer dst) {
//        int recSize = sizeOf(event);
//        if (recSize > dst.remaining()) {
//            return 0;
//        }
//
//        int bpos = dst.position();
//
//        long streamHash = StreamHasher.hash(event.stream());
//
//        byte[] evTypeBytes = StringUtils.toUtf8Bytes(event.type());
//        dst.putInt(recSize);
//        dst.putLong(streamHash);
//        dst.putInt(version);
//        dst.putInt(0); //tmp checksum
//        dst.putLong(sequence);
//        dst.putLong(System.currentTimeMillis());
//        dst.put((byte) 0); //TODO not in use (useful for tombstones) ?
//
//        dst.putShort((short) evTypeBytes.length);
//        dst.putInt(event.data().length);
//        dst.putShort((short) event.metadata().length);
//
//        dst.put(evTypeBytes);
//        dst.put(event.data());
//        dst.put(event.metadata());
//
//        writeChecksum(dst, bpos);
//
//        int copied = (dst.position() - bpos);
//
//        assert copied == recSize;
//        assert Event.isValid(dst, bpos);
//        return copied;
//    }

    public static int compare(ByteBuffer ev1, ByteBuffer ev2) {
        return IndexKey.compare(stream(ev1), version(ev1), stream(ev2), version(ev2));
    }

    public static String toString(ByteBuffer data) {
        if (!isValid(data)) {
            throw new IllegalArgumentException("Invalid event data");
        }
        return "RECORD_SIZE=" + sizeOf(data) + ", " +
                "STREAM_HASH=" + stream(data) + ", " +
                "VERSION=" + version(data) + ", " +
                "EVENT_TYPE=" + eventType(data) + ", " +
                "CHECKSUM=" + checksum(data) + ", " +
                "SEQUENCE=" + sequence(data) + ", " +
                "TIMESTAMP=" + timestamp(data) + ", " +
                "TYPE_LENGTH=" + eventTypeLen(data) + ", " +
                "DATA_LENGTH=" + dataLen(data);
    }

}
