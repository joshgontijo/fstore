package io.joshworks.eventry.server.cluster.client;

import io.joshworks.eventry.server.cluster.messages.ClusterMessage;
import org.jgroups.Address;
import org.jgroups.Message;

import java.nio.ByteBuffer;
import java.util.function.Function;

public class NodeMessage {

    public final Address address;
    public final ByteBuffer buffer;
    public final int code;

    public NodeMessage(Message message) {
        if (message == null) {
            this.address = null;
            this.buffer = null;
            this.code = Integer.MIN_VALUE;
        } else {
            this.address = message.src();
            this.buffer = ByteBuffer.wrap(message.buffer());
            this.code = buffer.getInt();
        }
    }

    public <T> T as(Function<ByteBuffer, T> type) {
        if (isError()) {
            throw new RuntimeException("Received error response from node");
        }
        return type.apply(buffer);
    }

    public boolean isEmpty() {
        return buffer == null;
    }

    public boolean isError() {
        return ClusterMessage.isError(buffer.getInt(0));
    }
}
