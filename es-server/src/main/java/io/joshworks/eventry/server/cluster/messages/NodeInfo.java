package io.joshworks.eventry.server.cluster.messages;

import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.fstore.serializer.collection.SetSerializer;

import java.nio.ByteBuffer;
import java.util.Set;

public class NodeInfo implements ClusterMessage {

    public static final int CODE = 19;

    private static final SetSerializer<Integer> setSerializer = new SetSerializer<>(Serializers.INTEGER, a -> Integer.BYTES);

    public final Set<Integer> partitions;


    public NodeInfo(Set<Integer> partitions) {
        this.partitions = partitions;
    }

    public NodeInfo(ByteBuffer bb) {
        this.partitions = setSerializer.fromBytes(bb);
    }

    @Override
    public byte[] toBytes() {
        int size = setSerializer.sizeOf(partitions);
        var bb = ByteBuffer.allocate(Integer.BYTES + size);
        bb.putInt(CODE);
        setSerializer.writeTo(partitions, bb);
        bb.flip();
        return bb.array();
    }

    @Override
    public String toString() {
        return "NodeInfo{" + "partitions=" + partitions + '}';
    }

    @Override
    public int code() {
        return CODE;
    }
}
