package io.joshworks.eventry.index;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;

public class IndexKeySerializer implements Serializer<IndexKey> {

    @Override
    public ByteBuffer toBytes(IndexKey data) {
        ByteBuffer bb = ByteBuffer.allocate(Long.BYTES + Integer.BYTES);
        writeTo(data, bb);
        return bb.flip();
    }

    @Override
    public void writeTo(IndexKey data, ByteBuffer dst) {
        dst.putLong(data.stream).putInt(data.version);
    }

    @Override
    public IndexKey fromBytes(ByteBuffer buffer) {
        return new IndexKey(buffer.getLong(), buffer.getInt());
    }
}
