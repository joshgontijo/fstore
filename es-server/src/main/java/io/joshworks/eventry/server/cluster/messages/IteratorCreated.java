package io.joshworks.eventry.server.cluster.messages;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.serializer.VStringSerializer;

import java.nio.ByteBuffer;

public class IteratorCreated implements ClusterMessage {

    public static final int CODE = 5;
    private static final Serializer<String> vStringSerializer = new VStringSerializer();

    public final String uuid;

    public IteratorCreated(String uuid) {
        this.uuid = uuid;
    }

    public IteratorCreated(ByteBuffer data) {
        this.uuid = vStringSerializer.fromBytes(data);
    }

    @Override
    public byte[] toBytes() {
        var bb = ByteBuffer.allocate(Integer.BYTES + VStringSerializer.sizeOf(uuid));
        bb.putInt(CODE);
        vStringSerializer.writeTo(uuid, bb);
        bb.flip();
        return bb.array();
    }
}
