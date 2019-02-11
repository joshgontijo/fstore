package io.joshworks.eventry.server.cluster.messages;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.fstore.serializer.VStringSerializer;
import io.joshworks.fstore.serializer.collection.ListSerializer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class NodeInfo implements ClusterMessage {

    public static final int CODE = 19;

    private static final ListSerializer<Integer> arraySerializer = new ListSerializer<>(Serializers.INTEGER, a -> Integer.BYTES, ArrayList::new);

    private final List<Integer> partitions;
    public final String nodeId;


    public NodeInfo(String nodeId, List<Integer> partitions) {
        this.nodeId = nodeId;
        this.partitions = partitions;
    }

    public NodeInfo(ByteBuffer bb) {
        this.nodeId = vStringSerializer.fromBytes(bb);
        this.partitions = arraySerializer.fromBytes(bb);
    }

    @Override
    public byte[] toBytes() {
        int size = arraySerializer.sizeOf(partitions);
        var bb = ByteBuffer.allocate(Integer.BYTES + VStringSerializer.sizeOf(nodeId));
        bb.putInt(CODE);
        vStringSerializer.writeTo(nodeId, bb);
        bb.flip();
        return bb.array();
    }
}
