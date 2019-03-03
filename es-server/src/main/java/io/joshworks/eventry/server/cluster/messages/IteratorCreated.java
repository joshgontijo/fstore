package io.joshworks.eventry.server.cluster.messages;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.serializer.VStringSerializer;

import java.nio.ByteBuffer;

public class IteratorCreated implements ClusterMessage {

    public static final int CODE = 5;
    private static final Serializer<String> vStringSerializer = new VStringSerializer();

    public final String iteratorId;

    public IteratorCreated(String iteratorId) {
        this.iteratorId = iteratorId;
    }

    public IteratorCreated(ByteBuffer data) {
        this.iteratorId = vStringSerializer.fromBytes(data);
    }

    @Override
    public byte[] toBytes() {
        var bb = ByteBuffer.allocate(Integer.BYTES + VStringSerializer.sizeOf(iteratorId));
        bb.putInt(CODE);
        vStringSerializer.writeTo(iteratorId, bb);
        bb.flip();
        return bb.array();
    }

    @Override
    public int code() {
        return CODE;
    }
}
