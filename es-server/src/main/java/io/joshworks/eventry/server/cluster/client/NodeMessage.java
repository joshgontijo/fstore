package io.joshworks.eventry.server.cluster.client;

import io.joshworks.eventry.server.cluster.messages.ClusterMessage;

import java.nio.ByteBuffer;
import java.util.function.Function;

public class NodeMessage {
    public final String nodeId;
    public final ByteBuffer buffer;
    public final int code;

    public NodeMessage(String nodeId, ByteBuffer buffer) {
        this.nodeId = nodeId;
        this.buffer = buffer;
        this.code = buffer.getInt();
    }

    public static NodeMessage create(String nodeId, ClusterMessage clusterMessage) {
        return new NodeMessage(nodeId, ByteBuffer.wrap(clusterMessage.toBytes()));
    }

    public <T> T as(Function<ByteBuffer, T> type) {
        if(isError()) {
            throw new RuntimeException("Received error response from node");
        }
        return type.apply(buffer);
    }

    public boolean isError() {
        return ClusterMessage.isError(buffer.getInt(0));
    }
}
