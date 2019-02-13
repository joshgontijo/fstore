package io.joshworks.eventry.server.cluster.messages;

import java.nio.ByteBuffer;

/**
 * Used to issue append to command to the partition owner by another node who received the message from the client
 */
public class AppendError implements ClusterMessage {

    public static final int CODE = -2;
    public final int errorCode;

    public AppendError(int errorCode) {
        this.errorCode = errorCode;
    }

    public AppendError(ByteBuffer data) {
        this.errorCode = data.getInt();
    }

    @Override
    public byte[] toBytes() {
        return ByteBuffer.allocate(Integer.BYTES * 2).putInt(CODE).putInt(errorCode).flip().array();
    }

    @Override
    public int code() {
        return CODE;
    }
}
