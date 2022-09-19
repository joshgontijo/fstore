package io.joshworks.es2;

import io.joshworks.es2.index.IndexKey;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.ByteBufferChecksum;
import io.joshworks.fstore.core.util.StringUtils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * <pre>
 *
 * RECORD_SIZE (4 BYTES)
 * CHECKSUM (4 BYTES)
 * ------------------
 * STREAM_HASH (8 BYTES)
 * VERSION (4 BYTES)
 * SEQUENCE (8 BYTES)
 * TIMESTAMP (8 BYTES)
 * TYPE_LENGTH (2 BYTES)
 * DATA_LENGTH (4 BYTES)
 *
 * EVENT_TYPE (N BYTES)
 * DATA (N BYTES)
 *
 *
 * </pre>
 *
 * <p><b>RECORD_SIZE</b>: The total size of the entry, <b>with</b> the RECORD_SIZE + CHECKSUM</p>
 * <br/>
 * <p><b>CHECKSUM</b>:  Computed from STREAM_HASH onwards (i.e. without RECORD_SIZE and CHECKSUM)</p>
 */
public class Event {

    public static final int NO_VERSION = -1;
    public static final int VERSION_TOO_HIGH = -22;

    public static final int HEADER_BYTES =
            Integer.BYTES + //RECORD_SIZE
                    Integer.BYTES + //CHECKSUM
                    Long.BYTES +    //STREAM_HASH
                    Integer.BYTES + //VERSION
                    Long.BYTES +    // SEQUENCE
                    Long.BYTES +    // TIMESTAMP
                    Short.BYTES +   //TYPE_LENGTH
                    Integer.BYTES;  //DATA_LENGTH

    private Event() {

    }

    private static final int SIZE_OFFSET = 0;
    private static final int CHECKSUM_OFFSET = SIZE_OFFSET + Integer.BYTES;

    private static final int STREAM_OFFSET = CHECKSUM_OFFSET + Integer.BYTES;
    private static final int VERSION_OFFSET = STREAM_OFFSET + Long.BYTES;
    private static final int SEQUENCE_OFFSET = VERSION_OFFSET + Integer.BYTES;
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

    public static int checksum(ByteBuffer data) {
        return data.getInt(data.position() + CHECKSUM_OFFSET);
    }

    public static int computeChecksum(ByteBuffer data) {
        var recSize = sizeOf(data);
        var checksumSize = recSize - (Integer.BYTES * 2); //RECORD_SIZE and CHECKSUM are not part of the checksum computation
        return ByteBufferChecksum.crc32(data, STREAM_OFFSET, checksumSize);
    }

    public static long timestamp(ByteBuffer data) {
        return data.getLong(data.position() + TIMESTAMP_OFFSET);
    }

    public static long sequence(ByteBuffer data) {
        return data.getLong(data.position() + SEQUENCE_OFFSET);
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

    public static String dataString(ByteBuffer data) {
        return new String(data(data), StandardCharsets.UTF_8);
    }

    public static byte[] data(ByteBuffer data) {
        int evTypeLen = eventTypeLen(data);
        int dataLen = dataLen(data);
        int offset = data.position() + EVENT_TYPE_OFFSET + evTypeLen;

        var dataBytes = new byte[dataLen];
        data.slice(offset, dataLen).get(dataBytes);
        return dataBytes;
    }

    public static int compare(ByteBuffer ev1, ByteBuffer ev2) {
        return IndexKey.compare(stream(ev1), version(ev1), stream(ev2), version(ev2));
    }

    public static boolean isValid(ByteBuffer data) {
        var recLen = sizeOf(data);
        if (recLen < 0 || recLen > data.remaining()) {
            return false;
        }
        return checksum(data) == computeChecksum(data);
    }


    //WRITES
    public static void writeVersion(ByteBuffer data, int version) {
        data.putInt(data.position() + VERSION_OFFSET, version);
    }

    public static void writeTimestamp(ByteBuffer data, long timestamp) {
        data.putLong(data.position() + TIMESTAMP_OFFSET, timestamp);
    }

    public static void writeSequence(ByteBuffer data, long sequence) {
        data.putLong(data.position() + SEQUENCE_OFFSET, sequence);
    }

    public static void writeChecksum(ByteBuffer data) {
        int checksum = computeChecksum(data);
        data.putInt(data.position() + CHECKSUM_OFFSET, checksum);
    }


    /**
     * Creates an valid event, with timestamp and checksum, but with a sequence field of zero.
     * Event though this event is valid, it should be only written to the log by using {@link BatchWriter} which sets the fields appropriately
     */
    public static ByteBuffer create(long stream, int version, String eventType, byte[] data) {
        var evType = StringUtils.toUtf8Bytes(eventType);
        var recSize = Event.HEADER_BYTES + evType.length + data.length;
        var buffer = Buffers.allocate(recSize, false);

        buffer.putInt(SIZE_OFFSET, recSize);
        buffer.putLong(STREAM_OFFSET, stream);
        buffer.putLong(TIMESTAMP_OFFSET, System.currentTimeMillis());
        buffer.putInt(VERSION_OFFSET, version);
        buffer.putShort(EVENT_TYPE_LENGTH_OFFSET, (short) evType.length);
        buffer.putInt(DATA_LENGTH_OFFSET, data.length);


        Buffers.offsetPosition(buffer, Event.HEADER_BYTES);
        buffer.put(evType);
        buffer.put(data);

        buffer.clear();
        writeChecksum(buffer);
        return buffer;
    }


    public static String toString(ByteBuffer data) {
        return "RECORD_SIZE=" + sizeOf(data) + ", " +
                "CHECKSUM=" + checksum(data) + ", " +
                "TIMESTAMP=" + timestamp(data) + ", " +
                "STREAM_HASH=" + stream(data) + ", " +
                "VERSION=" + version(data) + ", " +
                "TYPE_LENGTH=" + eventTypeLen(data) + ", " +
                "DATA_LENGTH=" + dataLen(data) + ", " +
                "EVENT_TYPE=" + eventType(data) + ", " +
                "DATA_STRING=" + dataString(data);

    }

}
