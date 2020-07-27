package io.joshworks.es;

import io.joshworks.es.events.WriteEvent;
import io.joshworks.es.index.IndexKey;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.ByteBufferChecksum;
import io.joshworks.fstore.core.util.StringUtils;

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
 * ATTRIBUTES (1 BYTE)
 *
 * TYPE_LENGTH (2 BYTES)
 * DATA_LENGTH (4 BYTES)
 * METADATA_LENGTH (2 BYTES)
 *
 * EVENT_TYPE (N BYTES)
 * DATA (N BYTES)
 * METADATA (N BYTES)
 *
 * </pre>
 */
public class Event {

    public static final int OVERHEAD =
            Integer.BYTES +  //RECORD_SIZE
                    Long.BYTES + //STREAM_HASH
                    Integer.BYTES + //VERSION
                    Integer.BYTES +  //CHECKSUM
                    Long.BYTES + // SEQUENCE
                    Long.BYTES + // TIMESTAMP
                    Byte.BYTES + //ATTRIBUTES
                    Short.BYTES + //TYPE_LENGTH
                    Integer.BYTES + //DATA_LENGTH
                    Short.BYTES; //METADATA_LENGTH

    private Event() {

    }

    private static final int SIZE_OFFSET = 0;
    private static final int STREAM_OFFSET = SIZE_OFFSET + Integer.BYTES;
    private static final int VERSION_OFFSET = STREAM_OFFSET + Long.BYTES;
    private static final int CHECKSUM_OFFSET = VERSION_OFFSET + Integer.BYTES;
    private static final int SEQUENCE_OFFSET = CHECKSUM_OFFSET + Integer.BYTES;
    private static final int TIMESTAMP_OFFSET = SEQUENCE_OFFSET + Long.BYTES;
    private static final int ATTRIBUTES_OFFSET = TIMESTAMP_OFFSET + Long.BYTES;
    private static final int EVENT_TYPE_LENGTH_OFFSET = ATTRIBUTES_OFFSET + Byte.BYTES;
    private static final int DATA_LENGTH_OFFSET = EVENT_TYPE_LENGTH_OFFSET + Short.BYTES;
    private static final int METADATA_LENGTH_OFFSET = DATA_LENGTH_OFFSET + Integer.BYTES;
    private static final int EVENT_TYPE_OFFSET = METADATA_LENGTH_OFFSET + Short.BYTES;


    public static int sizeOf(ByteBuffer data) {
        return sizeOf(data, data.position());
    }

    public static int sizeOf(ByteBuffer data, int offset) {
        return data.getInt(offset + SIZE_OFFSET);
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
        return checksum(data, data.position());
    }

    public static int checksum(ByteBuffer data, int offset) {
        return data.getInt(offset + CHECKSUM_OFFSET);
    }

    public static byte attributes(ByteBuffer data) {
        return data.get(data.position() + ATTRIBUTES_OFFSET);
    }

    public static short eventTypeLen(ByteBuffer data) {
        return data.getShort(data.position() + EVENT_TYPE_LENGTH_OFFSET);
    }

    public static int dataLen(ByteBuffer data) {
        return data.getInt(data.position() + DATA_LENGTH_OFFSET);
    }

    public static int metadataLen(ByteBuffer data) {
        return data.getInt(data.position() + METADATA_LENGTH_OFFSET);
    }

    public static String eventType(ByteBuffer data) {
        int evTypeLen = eventTypeLen(data);
        int offset = data.position() + EVENT_TYPE_OFFSET;
        return Buffers.toString(data, offset, evTypeLen);
    }

    public static boolean hasAttribute(ByteBuffer data, int attribute) {
        short attr = attributes(data);
        return (attr & (1 << attribute)) == 1;
    }

    private static byte attribute(int... attributes) {
        byte b = 0;
        for (int attr : attributes) {
            b = (byte) (b | 1 << attr);
        }
        return b;
    }

    public static boolean isValid(ByteBuffer data) {
        return isValid(data, data.position());
    }

    static boolean isValid(ByteBuffer recData, int offset) {
        if (!canReadRecordSize(recData, offset)) {
            return false;
        }

        int recSize = sizeOf(recData, offset);
        if (recSize <= 0 || recSize > Buffers.remaining(recData, offset)) {
            return false;
        }

        int checksum = checksum(recData, offset);
        int computed = computeChecksum(recData, offset, recSize);

        return computed == checksum;
    }

    private static int computeChecksum(ByteBuffer data, int offset, int recSize) {
        int chksOffset = offset + SEQUENCE_OFFSET;
        int chksLen = recSize - (chksOffset - offset);
        return ByteBufferChecksum.crc32(data, chksOffset, chksLen);
    }

    public static void writeChecksum(ByteBuffer data, int offset) {
        int recSize = Event.sizeOf(data, offset);
        int checksum = computeChecksum(data, offset, recSize);
        data.putInt(offset + CHECKSUM_OFFSET, checksum);

    }

    private static boolean canReadRecordSize(ByteBuffer recData, int offset) {
        return Buffers.remaining(recData, offset) >= Integer.BYTES;
    }

    public static void writeSequence(ByteBuffer data, int offset, long sequence) {
        data.putLong(offset + SEQUENCE_OFFSET, sequence);
    }

    public static void writeVersion(ByteBuffer data, int offset, int version) {
        data.putInt(offset + VERSION_OFFSET, version);
    }

    public static void rewrite(ByteBuffer data, int offset, long stream, int version) {
        data.putLong(offset + STREAM_OFFSET, stream);
        writeVersion(data, offset, version);
        assert Event.isValid(data, offset);
    }

    public static int sizeOf(WriteEvent event) {
        return OVERHEAD +
                StringUtils.utf8Length(event.type()) +
                event.data().length +
                event.metadata().length;
    }

    public static int serialize(WriteEvent event, int version, long sequence, ByteBuffer dst) {
        int recSize = sizeOf(event);
        if (recSize > dst.remaining()) {
            return 0;
        }

        int bpos = dst.position();

        long streamHash = StreamHasher.hash(event.stream());

        byte[] evTypeBytes = StringUtils.toUtf8Bytes(event.type());
        dst.putInt(recSize);
        dst.putLong(streamHash);
        dst.putInt(version);
        dst.putInt(0); //tmp checksum
        dst.putLong(sequence);
        dst.putLong(System.currentTimeMillis());
        dst.put((byte) 0); //TODO not in use (useful for tombstones) ?

        dst.putShort((short) evTypeBytes.length);
        dst.putInt(event.data().length);
        dst.putShort((short) event.metadata().length);

        dst.put(evTypeBytes);
        dst.put(event.data());
        dst.put(event.metadata());

        writeChecksum(dst, bpos);

        int copied = (dst.position() - bpos);

        assert copied == recSize;
        assert Event.isValid(dst, bpos);
        return copied;
    }

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
                "ATTRIBUTES=" + attributes(data) + ", " +
                "TYPE_LENGTH=" + eventTypeLen(data) + ", " +
                "DATA_LENGTH=" + dataLen(data) + ", " +
                "METADATA_LENGTH=" + metadataLen(data);
    }

}
