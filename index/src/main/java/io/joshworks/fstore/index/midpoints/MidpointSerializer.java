package io.joshworks.fstore.index.midpoints;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;

public class MidpointSerializer<K extends Comparable<K>> implements Serializer<Midpoint<K>> {

    private final Serializer<K> keySerializer;

    public MidpointSerializer(Serializer<K> keySerializer) {
        this.keySerializer = keySerializer;
    }

    @Override
    public ByteBuffer toBytes(Midpoint<K> data) {
        ByteBuffer keyData = keySerializer.toBytes(data.key);
        ByteBuffer bb = ByteBuffer.allocate(keyData.limit() + Long.BYTES);
        bb.put(keyData);
        bb.putLong(data.position);
        return bb.flip();
    }

    @Override
    public void writeTo(Midpoint<K> data, ByteBuffer dst) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Midpoint<K> fromBytes(ByteBuffer buffer) {
        K key = keySerializer.fromBytes(buffer);
        long blockPos = buffer.getLong();
        return new Midpoint<>(key, blockPos);
    }
}
