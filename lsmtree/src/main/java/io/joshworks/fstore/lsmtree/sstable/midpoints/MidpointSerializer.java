package io.joshworks.fstore.lsmtree.sstable.midpoints;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;

public class MidpointSerializer<K extends Comparable<K>> implements Serializer<Midpoint<K>> {

    private final Serializer<K> keySerializer;

    public MidpointSerializer(Serializer<K> keySerializer) {
        this.keySerializer = keySerializer;
    }

    @Override
    public void writeTo(Midpoint<K> data, ByteBuffer dst) {
        keySerializer.writeTo(data.key, dst);
        dst.putLong(data.position);
    }

    @Override
    public Midpoint<K> fromBytes(ByteBuffer buffer) {
        K key = keySerializer.fromBytes(buffer);
        long blockPos = buffer.getLong();
        return new Midpoint<>(key, blockPos);
    }
}
