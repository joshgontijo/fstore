package io.joshworks.fstore.log.segment.header;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;

public class DeletedHeaderSerializer implements Serializer<DeletedHeader> {

    @Override
    public ByteBuffer toBytes(DeletedHeader data) {
        ByteBuffer bb = ByteBuffer.allocate(Header.BYTES);
        writeTo(data, bb);
        return bb.flip();
    }

    @Override
    public void writeTo(DeletedHeader data, ByteBuffer dest) {
        dest.putLong(data.timestamp);
    }

    @Override
    public DeletedHeader fromBytes(ByteBuffer buffer) {
        long timestamp = buffer.getLong();
        return new DeletedHeader(timestamp);
    }
}
