package io.joshworks.eventry.server.cluster.messages;

import io.joshworks.eventry.LinkToPolicy;
import io.joshworks.eventry.SystemEventPolicy;

import java.nio.ByteBuffer;

public class FromAll implements ClusterMessage {

    public static final int CODE = 1;
    public static final int BYTES = Integer.BYTES * 4;

    private static final LinkToPolicy[] ltpItems = LinkToPolicy.values();
    private static final SystemEventPolicy[] sepItems = SystemEventPolicy.values();

    public final int timeout;//seconds
    public final int batchSize;

    public final LinkToPolicy linkToPolicy;
    public final SystemEventPolicy systemEventPolicy;


    public FromAll(int timeout, int batchSize, LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy) {
        this.timeout = timeout;
        this.batchSize = batchSize;
        this.linkToPolicy = linkToPolicy;
        this.systemEventPolicy = systemEventPolicy;
    }

    public FromAll(ByteBuffer bb) {
        this.timeout = bb.getInt();
        this.batchSize = bb.getInt();
        this.linkToPolicy = ltpItems[bb.getInt()];
        this.systemEventPolicy = sepItems[bb.getInt()];
    }

    @Override
    public byte[] toBytes() {
        ByteBuffer bb = ByteBuffer.allocate(BYTES);
        bb.putInt(CODE);
        bb.putInt(timeout);
        bb.putInt(batchSize);
        bb.putInt(linkToPolicy.ordinal());
        bb.putInt(systemEventPolicy.ordinal());
        return bb.flip().array();
    }
}
