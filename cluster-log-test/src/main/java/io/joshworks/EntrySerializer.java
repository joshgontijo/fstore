package io.joshworks;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;

class EntrySerializer<T> implements Serializer<Entry<T>> {

    private final Serializer<T> dataSerializer;

    EntrySerializer(Serializer<T> dataSerializer) {
        this.dataSerializer = dataSerializer;
    }

    @Override
    public void writeTo(Entry<T> data, ByteBuffer dst) {
        dst.putLong(data.sequence);
        dataSerializer.writeTo(data.data, dst);
    }

    @Override
    public Entry<T> fromBytes(ByteBuffer buffer) {
        long idx = buffer.getLong();
        T data = dataSerializer.fromBytes(buffer);
        return new Entry<>(idx, data);
    }
}
