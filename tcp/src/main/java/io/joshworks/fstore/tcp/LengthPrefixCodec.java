package io.joshworks.fstore.tcp;

import io.joshworks.fstore.tcp.conduits.FramingMessageSourceConduit;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Format accepted by {@link FramingMessageSourceConduit}
 */
public class LengthPrefixCodec {

    public static void serialize(Object data, ByteBuffer dst) {
        if (dst.remaining() < Integer.BYTES) {
            throw new IllegalStateException("Buffer must be at least " + Integer.BYTES);
        }
        int pos = dst.position();
        dst.position(pos + Integer.BYTES);
//        KryoStoreSerializer.serialize(data, dst);
        byte[] d = new byte[10];
        Arrays.fill(d, (byte) 1);
        dst.put(d);

        dst.putInt(pos, dst.position() - pos - Integer.BYTES); //does not include the length length itself
    }
}
