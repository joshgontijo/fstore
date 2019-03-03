package io.joshworks.eventry.server.cluster.messages;

import java.nio.ByteBuffer;

/**
 * Used to issue append to command to the partition owner by another node who received the message from the client
 */
public class UnknownMessage implements ClusterMessage {

    public static final int CODE = -1;
    public final int unknownCode;

    public UnknownMessage(int unknownCode) {
        this.unknownCode = unknownCode;
    }

    public UnknownMessage(ByteBuffer data) {
        this.unknownCode = data.getInt();
    }

    @Override
    public byte[] toBytes() {
        return ByteBuffer.allocate(Integer.BYTES * 2).putInt(CODE).putInt(unknownCode).flip().array();
    }

    @Override
    public int code() {
        return CODE;
    }
}
