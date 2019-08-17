package io.joshworks.eventry.server.tcp_xnio.tcp;

import io.joshworks.fstore.serializer.kryo.KryoStoreSerializer;

import java.nio.ByteBuffer;

/**
 * Format accepted by {@link io.joshworks.eventry.server.tcp_xnio.tcp.conduits.FramingMessageSourceConduit}
 */
public class LengthPrefixCodec {

    public static void write(Object data, ByteBuffer dst) {
        if (dst.remaining() < Integer.BYTES) {
            throw new IllegalStateException("Buffer must be at least " + Integer.BYTES);
        }
        int pos = dst.position();
        dst.position(pos + Integer.BYTES);
        KryoStoreSerializer.serialize(data, dst);
        dst.putInt(pos, dst.position() - pos - Integer.BYTES); //does not include the length length itself
    }
}
