package io.joshworks.eventry.network.tcp;

import io.joshworks.eventry.network.tcp.conduits.FramingMessageSourceConduit;
import io.joshworks.fstore.serializer.json.JsonSerializer;
import io.joshworks.fstore.serializer.kryo.KryoStoreSerializer;

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
