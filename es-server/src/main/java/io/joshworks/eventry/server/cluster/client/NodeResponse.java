package io.joshworks.eventry.server.cluster.client;

import io.joshworks.eventry.server.cluster.commands.ClusterMessage;

import java.nio.ByteBuffer;

public class NodeResponse {
    public final String uuid;
    public final ByteBuffer buffer;
    public final int code;

    public NodeResponse(String uuid, ByteBuffer buffer) {
        this.uuid = uuid;
        this.buffer = buffer;
        this.code = buffer.getInt();
    }

    public boolean isError() {
        return ClusterMessage.isError(buffer.getInt(0));
    }
}
