package io.joshworks.fstore.log.segment.header;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.serializer.Serializers;

import java.nio.ByteBuffer;

public class CreatedHeaderSerializer implements Serializer<CreatedHeader> {

    @Override
    public ByteBuffer toBytes(CreatedHeader data) {
        ByteBuffer bb = ByteBuffer.allocate(Header.BYTES);
        writeTo(data, bb);
        return bb.flip();
    }

    @Override
    public void writeTo(CreatedHeader data, ByteBuffer dest) {
        Serializers.VSTRING.writeTo(data.magic, dest);
        dest.putLong(data.created);
        dest.putInt(data.type.val);
        dest.putLong(data.fileSize);
    }

    @Override
    public CreatedHeader fromBytes(ByteBuffer buffer) {
        String magic = Serializers.VSTRING.fromBytes(buffer);
        long created = buffer.getLong();
        int type = buffer.getInt();
        long fileSize = buffer.getLong();

        return new CreatedHeader(magic, created, Type.of(type), fileSize);
    }
}
