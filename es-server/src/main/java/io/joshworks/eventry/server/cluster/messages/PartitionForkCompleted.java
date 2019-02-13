package io.joshworks.eventry.server.cluster.messages;

import java.nio.ByteBuffer;

public class PartitionForkCompleted implements ClusterMessage {

    public static final int CODE = 16;

    public final int partitionId;

    public PartitionForkCompleted(int partitionId) {
        this.partitionId = partitionId;
    }

    public PartitionForkCompleted(ByteBuffer bb) {
        this.partitionId = bb.getInt();
    }

    @Override
    public byte[] toBytes() {
        var bb = ByteBuffer.allocate(Integer.BYTES * 2);
        bb.putInt(CODE);
        bb.putInt(partitionId);
        bb.flip();
        return bb.array();
    }

    @Override
    public int code() {
        return CODE;
    }
}