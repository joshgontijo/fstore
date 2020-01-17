package io.joshworks.fstore.tcp;

import java.nio.ByteBuffer;

public class TcpMessage {

    //LENGTHS
    private static final int MESSAGE_LEN_LEN = Integer.BYTES;
    private static final int FLAGS_LEN = Short.BYTES;
    public static final int BYTES = MESSAGE_LEN_LEN + FLAGS_LEN;

    //OFFSETS
    private static final int MESSAGE_OFFSET = 0;
    private static final int FLAGS_OFFSET = Integer.BYTES;

    //COMPRESSION BITS
    private static final int FLAG_COMPR_BIT_SHIFT = 0;
    private static final int FLAG_COMPR_BIT_LEN = Compression.values().length;

    //REQUEST_RESPONSE BITS
    private static final int FLAG_REQRES_BIT_SHIFT = FLAG_COMPR_BIT_SHIFT + FLAG_COMPR_BIT_LEN;
    private static final int FLAG_REQRES_BIT_LEN = RequestResponse.values().length;

    //PAYLOAD_TYPE BITS
    private static final int FLAG_PTYPE_BIT_SHIFT = FLAG_REQRES_BIT_SHIFT + FLAG_REQRES_BIT_LEN;
    private static final int FLAG_PTYPE_BIT_LEN = Payload.values().length;

    //INTERNAL BITS
    private static final int FLAG_INTERNAL_BIT_SHIFT = FLAG_PTYPE_BIT_SHIFT + FLAG_PTYPE_BIT_LEN;
    private static final int FLAG_INTERNAL_BIT_LEN = Internal.values().length;


    public static int messageLength(ByteBuffer buffer) {
        return buffer.getInt(MESSAGE_OFFSET);
    }

    private static short readFlag(ByteBuffer buffer) {
        return buffer.getShort(FLAGS_OFFSET);
    }


    public static void compression(ByteBuffer buffer, Compression compression) {
        short flag = readFlags(buffer);
        short updated = (short) (flag | (compression.val << FLAG_COMPR_BIT_SHIFT));
        setFlag(buffer, updated);
    }

    public static void requestResponse(ByteBuffer buffer, RequestResponse requestResponse) {
        short flag = readFlags(buffer);
        short updated = (short) (flag | (requestResponse.val << FLAG_REQRES_BIT_SHIFT));
        setFlag(buffer, updated);
    }

    public static void payloadType(ByteBuffer buffer, Payload payload) {
        short flag = readFlags(buffer);
        short updated = (short) (flag | (payload.val << FLAG_PTYPE_BIT_SHIFT));
        setFlag(buffer, updated);
    }

    public static void internal(ByteBuffer buffer, Internal internal) {
        short flag = readFlags(buffer);
        short updated = (short) (flag | (internal.val << FLAG_INTERNAL_BIT_SHIFT));
        setFlag(buffer, updated);
    }

    public static Compression compression(ByteBuffer buffer) {
        short flag = readFlags(buffer);
        int i = readBits(flag, FLAG_COMPR_BIT_SHIFT, FLAG_COMPR_BIT_LEN);
        if (Compression.NONE.val == i) {
            return Compression.NONE;
        }
        if (Compression.SNAPPY.val == i) {
            return Compression.SNAPPY;
        }
        if (Compression.LZ4_FAST.val == i) {
            return Compression.LZ4_FAST;
        }
        if (Compression.LZ4_HIGH.val == i) {
            return Compression.LZ4_HIGH;
        }
        if (Compression.DEFLATE.val == i) {
            return Compression.SNAPPY;
        }
        throw new IllegalArgumentException("Invalid compression header");
    }

    public static RequestResponse requestResponse(ByteBuffer buffer) {
        short flag = readFlags(buffer);
        int i = readBits(flag, FLAG_REQRES_BIT_SHIFT, FLAG_REQRES_BIT_LEN);
        if (RequestResponse.NONE.val == i) {
            return RequestResponse.NONE;
        }
        if (RequestResponse.REQUEST.val == i) {
            return RequestResponse.REQUEST;
        }
        if (RequestResponse.RESPONSE.val == i) {
            return RequestResponse.RESPONSE;
        }
        throw new IllegalArgumentException("Invalid request response header");
    }

    public static Payload payloadType(ByteBuffer buffer) {
        short flag = readFlags(buffer);
        int i = readBits(flag, FLAG_PTYPE_BIT_SHIFT, FLAG_PTYPE_BIT_LEN);
        if (Payload.RAW.val == i) {
            return Payload.RAW;
        }
        if (Payload.OBJECT.val == i) {
            return Payload.OBJECT;
        }
        if (Payload.RPC.val == i) {
            return Payload.RPC;
        }
        throw new IllegalArgumentException("Invalid payload type header");
    }

    public static Internal internal(ByteBuffer buffer) {
        short flag = readFlags(buffer);
        int i = readBits(flag, FLAG_PTYPE_BIT_SHIFT, FLAG_PTYPE_BIT_LEN);
        if (Internal.ACK.val == i) {
            return Internal.ACK;
        }
        if (Internal.DISCONNECT.val == i) {
            return Internal.DISCONNECT;
        }
        if (Internal.KEEP_ALIVE.val == i) {
            return Internal.KEEP_ALIVE;
        }
        if (Internal.PING.val == i) {
            return Internal.PING;
        }
        if (Internal.PONG.val == i) {
            return Internal.PONG;
        }
        throw new IllegalArgumentException("Invalid internal message header");
    }

    private static short readFlags(ByteBuffer buffer) {
        return buffer.getShort(buffer.position() + FLAGS_OFFSET);
    }

    private static void setFlag(ByteBuffer buffer, short flag) {
        buffer.putShort(buffer.position() + FLAGS_OFFSET, flag);
    }


    private static int readBits(short flags, int shift, int len) {
        int mask = (1 << len) - 1;
        return mask & (flags >>> shift);
    }

    public enum Compression {
        NONE(1),
        LZ4_FAST(1 << 1),
        LZ4_HIGH(1 << 2),
        SNAPPY(1 << 3),
        DEFLATE(1 << 4);

        private final int val;

        Compression(int val) {
            this.val = val;
        }
    }

    public enum RequestResponse {
        NONE(1),
        REQUEST(1 << 1),
        RESPONSE(1 << 2);

        private final int val;

        RequestResponse(int val) {
            this.val = val;
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
    }
}
