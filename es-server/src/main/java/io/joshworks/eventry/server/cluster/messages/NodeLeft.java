package io.joshworks.eventry.server.cluster.messages;

import io.joshworks.fstore.serializer.VStringSerializer;

import java.nio.ByteBuffer;

public class NodeLeft implements ClusterMessage {

    public static final int CODE = 16;

    public final String nodeId;

    public NodeLeft(String nodeId) {
        this.nodeId = nodeId;
    }

    public NodeLeft(ByteBuffer bb) {
        this.nodeId = vStringSerializer.fromBytes(bb);
    }

    @Override
    public byte[] toBytes() {
        var bb = ByteBuffer.allocate(Integer.BYTES + VStringSerializer.sizeOf(nodeId));
        bb.putInt(CODE);
        vStringSerializer.writeTo(nodeId, bb);
        bb.flip();
        return bb.array();
    }

}
