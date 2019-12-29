package io.joshworks;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.serializer.Serializers;

import java.nio.ByteBuffer;

class EntrySerializer implements Serializer<Record> {

    private final Serializer<ByteBuffer> dataSerializer = Serializers.copy();

    @Override
    public void writeTo(Record data, ByteBuffer dst) {
        dst.putLong(data.sequence);
        dataSerializer.writeTo(data.data, dst);
    }

    @Override
    public Record fromBytes(ByteBuffer buffer) {
        long idx = buffer.getLong();
        ByteBuffer data = dataSerializer.fromBytes(buffer);
        return new Record(idx, data);
    }
}
