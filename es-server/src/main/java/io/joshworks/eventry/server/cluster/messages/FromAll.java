package io.joshworks.eventry.server.cluster.messages;

import io.joshworks.eventry.LinkToPolicy;
import io.joshworks.eventry.StreamName;
import io.joshworks.eventry.SystemEventPolicy;
import io.joshworks.fstore.serializer.VStringSerializer;

import java.nio.ByteBuffer;

public class FromAll implements ClusterMessage {

    public static final int CODE = 1;
    public static final int BYTES = Integer.BYTES * 6;

    private static final LinkToPolicy[] ltpItems = LinkToPolicy.values();
    private static final SystemEventPolicy[] sepItems = SystemEventPolicy.values();

    public final int timeout;//seconds
    public final int batchSize;
    public final int partitionId;

    public final String lastEvent;

    public final LinkToPolicy linkToPolicy;
    public final SystemEventPolicy systemEventPolicy;


    public FromAll(int timeout, int batchSize, int partitionId, LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy, StreamName lastEvent) {
        this.timeout = timeout;
        this.batchSize = batchSize;
        this.partitionId = partitionId;
        this.linkToPolicy = linkToPolicy;
        this.systemEventPolicy = systemEventPolicy;
        this.lastEvent = lastEvent == null ? "" : lastEvent.toString();
    }

    public FromAll(ByteBuffer bb) {
        this.timeout = bb.getInt();
        this.batchSize = bb.getInt();
        this.partitionId = bb.getInt();
        this.lastEvent = vStringSerializer.fromBytes(bb);
        this.linkToPolicy = ltpItems[bb.getInt()];
        this.systemEventPolicy = sepItems[bb.getInt()];
    }

    @Override
    public byte[] toBytes() {
        ByteBuffer bb = ByteBuffer.allocate(BYTES + VStringSerializer.sizeOf(lastEvent));
        bb.putInt(CODE);
        bb.putInt(timeout);
        bb.putInt(batchSize);
        bb.putInt(partitionId);
        vStringSerializer.writeTo(lastEvent, bb);
        bb.putInt(linkToPolicy.ordinal());
        bb.putInt(systemEventPolicy.ordinal());
        return bb.flip().array();
    }

    @Override
    public int code() {
        return CODE;
    }
}