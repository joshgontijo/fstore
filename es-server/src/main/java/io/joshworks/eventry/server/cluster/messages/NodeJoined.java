package io.joshworks.eventry.server.cluster.messages;

import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.fstore.serializer.VStringSerializer;
import io.joshworks.fstore.serializer.collection.SetSerializer;

import java.nio.ByteBuffer;
import java.util.Set;

public class NodeJoined implements ClusterMessage {

    public static final int CODE = 15;

    private static final SetSerializer<Integer> setSerializer = new SetSerializer<>(Serializers.INTEGER, a -> Integer.BYTES);

    public final String nodeId;
    public final Set<Integer> partitions;


    public NodeJoined(String nodeId, Set<Integer> partitions) {
        this.nodeId = nodeId;
        this.partitions = partitions;
    }

    public NodeJoined(ByteBuffer bb) {
        this.nodeId = vStringSerializer.fromBytes(bb);
        this.partitions = setSerializer.fromBytes(bb);
    }

    @Override
    public byte[] toBytes() {
        int size = setSerializer.sizeOf(partitions);
        var bb = ByteBuffer.allocate(Integer.BYTES + VStringSerializer.sizeOf(nodeId) + size);
        bb.putInt(CODE);
        vStringSerializer.writeTo(nodeId, bb);
        setSerializer.toBytes(partitions);
        bb.flip();
        return bb.array();
    }

    @Override
    public String toString() {
        return "NodeJoined{" + "nodeId='" + nodeId + '\'' +
                ", partitions=" + partitions +
                '}';
    }

    @Override
    public int code() {
        return CODE;
    }
}
