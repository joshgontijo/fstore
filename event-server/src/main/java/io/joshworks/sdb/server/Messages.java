package io.joshworks.sdb.server;

import io.joshworks.es2.StreamHasher;
import io.joshworks.fstore.serializer.json.JsonSerializer;

import java.nio.ByteBuffer;

import static io.joshworks.es2.Event.HEADER_BYTES;
import static io.joshworks.fstore.core.util.StringUtils.toUtf8Bytes;

/**
 * LEN (4 bytes)
 * OP (2 bytes)
 */
public class Messages {

    private static final int HEADER_LEN = Integer.BYTES + OP.LEN;

    public static ByteBuffer serializeAppend(String stream, String eventType, int version, Object event) {
        byte[] dataBytes = JsonSerializer.toBytes(event);
        byte[] typeBytes = toUtf8Bytes(eventType);
        long streamHash = StreamHasher.hash(stream);
        int recSize = dataBytes.length + typeBytes.length + HEADER_BYTES;

        var dst = ByteBuffer.allocate(HEADER_LEN + recSize);
        dst.position(HEADER_LEN);
        int bpos = dst.position();

        dst.putInt(recSize);
        dst.putLong(streamHash);
        dst.putInt(version);

        long ts = System.currentTimeMillis();
        dst.putLong(ts);

        dst.putShort((short) typeBytes.length);
        dst.putInt(dataBytes.length);
        dst.put(typeBytes);
        dst.put(dataBytes);

        int copied = (dst.position() - bpos);

        assert copied == recSize;
        return dst.flip();
    }


}
