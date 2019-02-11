package io.joshworks.eventry.server.cluster.commands;

import java.nio.ByteBuffer;

/**
 * Used to issue append to command to the partition owner by another node who received the message from the client
 */
public class AppendSuccess implements ClusterMessage {

    public static final int CODE = 3;

    @Override
    public byte[] toBytes() {
        return ByteBuffer.allocate(Integer.BYTES).putInt(CODE).flip().array();
    }
}
