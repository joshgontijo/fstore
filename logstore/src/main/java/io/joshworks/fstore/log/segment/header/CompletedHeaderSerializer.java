package io.joshworks.fstore.log.segment.header;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.serializer.Serializers;

import java.nio.ByteBuffer;

public class CompletedHeaderSerializer implements Serializer<CompletedHeader> {

    @Override
    public ByteBuffer toBytes(CompletedHeader data) {
        ByteBuffer bb = ByteBuffer.allocate(Header.BYTES);
        writeTo(data, bb);
        return bb.flip();
    }

    @Override
    public void writeTo(CompletedHeader data, ByteBuffer dest) {
        dest.putInt(data.level);
        dest.putLong(data.entries);
        dest.putLong(data.logicalSize);
        dest.putLong(data.timestamp);
    }

    @Override
    public CompletedHeader fromBytes(ByteBuffer buffer) {
        int level = buffer.getInt();
        long entries = buffer.getLong();
        long logicalSize = buffer.getLong();
        long timestamp = buffer.getLong();

        return new CompletedHeader(entries, level, timestamp, logicalSize);
    }
}
