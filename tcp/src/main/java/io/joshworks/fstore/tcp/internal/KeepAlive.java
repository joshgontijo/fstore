package io.joshworks.fstore.tcp.internal;

import io.joshworks.fstore.serializer.kryo.KryoSerializer;

public class KeepAlive {
    public static final byte[] DATA = KryoSerializer.serialize(new KeepAlive());
}
