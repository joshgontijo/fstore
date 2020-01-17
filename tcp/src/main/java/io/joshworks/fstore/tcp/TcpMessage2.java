package io.joshworks.fstore.tcp;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;

public class TcpMessage2 {

    public static final int BYTES = Integer.BYTES + (Short.BYTES * 4);

    private static final int OFFSET_MESSAGE = 0;
    private static final int OFFSET_MTYPE = OFFSET_MESSAGE + Integer.BYTES;
    private static final int OFFSET_COMPRESSION = OFFSET_MTYPE + Short.BYTES;
    private static final int OFFSET_PAYLOAD = OFFSET_COMPRESSION + Short.BYTES;
    private static final int OFFSET_INTERNAL = OFFSET_PAYLOAD + Short.BYTES;

    private static final Compression[] compression = Arrays.stream(Compression.values()).sorted(Comparator.comparingInt(Compression::val)).toArray(Compression[]::new);
    private static final RequestResponse[] requestResponse = Arrays.stream(RequestResponse.values()).sorted(Comparator.comparingInt(RequestResponse::val)).toArray(RequestResponse[]::new);
    private static final Payload[] payload = Arrays.stream(Payload.values()).sorted(Comparator.comparingInt(Payload::val)).toArray(Payload[]::new);
    private static final Internal[] internal = Arrays.stream(Internal.values()).sorted(Comparator.comparingInt(Internal::val)).toArray(Internal[]::new);


    public static int messageLength(ByteBuffer buffer) {
        return buffer.getInt(OFFSET_MESSAGE);
    }

    public static void compression(ByteBuffer buffer, Compression compression) {
        buffer.putShort(buffer.position() + OFFSET_COMPRESSION, (short) compression.val);
    }

    public static void messageType(ByteBuffer buffer, RequestResponse requestResponse) {
        buffer.putShort(buffer.position() + OFFSET_MTYPE, (short) requestResponse.val);
    }

    public static void payloadType(ByteBuffer buffer, Payload payload) {
        buffer.putShort(buffer.position() + OFFSET_PAYLOAD, (short) payload.val);
    }

    public static void internal(ByteBuffer buffer, Internal internal) {
        buffer.putShort(buffer.position() + OFFSET_INTERNAL, (short) internal.val);
    }

    public static Compression compression(ByteBuffer buffer) {
        return compression[readFlag(buffer, OFFSET_COMPRESSION)];
    }

    public static RequestResponse messageType(ByteBuffer buffer) {
        return requestResponse[readFlag(buffer, OFFSET_MTYPE)];
    }

    public static Payload payloadType(ByteBuffer buffer) {
        return payload[readFlag(buffer, OFFSET_PAYLOAD)];
    }

    public static Internal internal(ByteBuffer buffer) {
        return internal[readFlag(buffer, OFFSET_INTERNAL)];
    }

    private static int readFlag(ByteBuffer buffer, int offset) {
        return buffer.getShort(buffer.position() + offset);
    }

    public enum Compression {
        NONE(1),
        LZ4_FAST(2),
        LZ4_HIGH(3),
        SNAPPY(4),
        DEFLATE(5);

        private final int val;

        Compression(int val) {
            this.val = val;
        }

        public int val() {
            return val;
        }
    }

    public enum RequestResponse {
        NONE(1),
        REQUEST(2),
        RESPONSE(3);

        private final int val;

        RequestResponse(int val) {
            this.val = val;
        }

        public int val() {
            return val;
        }
    }

    public enum Payload {
        RAW(1),
        OBJECT(1 << 1),
        RPC(1 << 2);

        private final int val;

        Payload(int val) {
            this.val = val;
        }

        public int val() {
            return val;
        }
    }

    public enum Internal {
        PING(1),
        PONG(1 << 1),
        KEEP_ALIVE(1 << 2),
        DISCONNECT(1 << 3),
        ACK(1 << 4);

        private final int val;

        Internal(int val) {
            this.val = val;
        }

        public int val() {
            return val;
        }
    }
}
