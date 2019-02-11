package io.joshworks.eventry.server.cluster.messages;

import java.nio.ByteBuffer;


public class Ack implements ClusterMessage {

    public static final int CODE = 100;

    @Override
    public byte[] toBytes() {
        return ByteBuffer.allocate(Integer.BYTES).putInt(CODE).flip().array();
    }
}
