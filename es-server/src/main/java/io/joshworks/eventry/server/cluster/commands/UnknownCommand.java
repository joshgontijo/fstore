package io.joshworks.eventry.server.cluster.commands;

import java.nio.ByteBuffer;

/**
 * Used to issue append to command to the partition owner by another node who received the message from the client
 */
public class UnknownCommand implements ClusterMessage {

    public static final int CODE = -1;
    public final int unknownCode;

    public UnknownCommand(int unknownCode) {
        this.unknownCode = unknownCode;
    }

    public UnknownCommand(ByteBuffer data) {
        this.unknownCode = data.getInt();
    }

    @Override
    public byte[] toBytes() {
        return ByteBuffer.allocate(Integer.BYTES * 2).putInt(CODE).putInt(unknownCode).flip().array();
    }
}
