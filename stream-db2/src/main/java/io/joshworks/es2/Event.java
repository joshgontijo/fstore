package io.joshworks.es2;

import io.joshworks.fstore.core.io.buffers.Buffers;

import java.nio.ByteBuffer;

/**
 * <pre>
 *
 * RECORD_SIZE (4 BYTES)
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
 * </pre>
 */
public class Event {

    public static final int NO_VERSION = -1;

    public static final int HEADER_BYTES =
            Integer.BYTES +  //RECORD_SIZE
                    Long.BYTES + //STREAM_HASH
                    Integer.BYTES + //VERSION
                    Long.BYTES + // SEQUENCE
                    Long.BYTES + // TIMESTAMP
                    Short.BYTES + //TYPE_LENGTH
                    Integer.BYTES; //DATA_LENGTH

    private Event() {

    }

    private static final int SIZE_OFFSET = 0;
    private static final int STREAM_OFFSET = SIZE_OFFSET + Integer.BYTES;
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

    public static long sequence(ByteBuffer data) {
        return data.getLong(data.position() + SEQUENCE_OFFSET);
    }

    public static long timestamp(ByteBuffer data) {
        return data.getLong(data.position() + TIMESTAMP_OFFSET);
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

    public static int compare(ByteBuffer ev1, ByteBuffer ev2) {
        return IndexKey.compare(stream(ev1), version(ev1), stream(ev2), version(ev2));
    }

    public static String toString(ByteBuffer data) {
        return "RECORD_SIZE=" + sizeOf(data) + ", " +
                "STREAM_HASH=" + stream(data) + ", " +
                "VERSION=" + version(data) + ", " +
                "EVENT_TYPE=" + eventType(data) + ", " +
                "SEQUENCE=" + sequence(data) + ", " +
                "TIMESTAMP=" + timestamp(data) + ", " +
                "TYPE_LENGTH=" + eventTypeLen(data) + ", " +
                "DATA_LENGTH=" + dataLen(data);
    }

}
