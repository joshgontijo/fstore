package io.joshworks.fstore.lsmtree.sstable;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;

public class EntrySerializer<K extends Comparable<K>, V> implements Serializer<Entry<K, V>> {

    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;

    public EntrySerializer(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }

    @Override
    public void writeTo(Entry<K, V> data, ByteBuffer dst) {
        keySerializer.writeTo(data.key, dst);
        if (data.value != null) {
            valueSerializer.writeTo(data.value, dst);
        }
    }

    @Override
    public Entry<K, V> fromBytes(ByteBuffer buffer) {
        K k = keySerializer.fromBytes(buffer);
        V v = buffer.hasRemaining() ? valueSerializer.fromBytes(buffer) : null;
        return Entry.of(k, v);
    }
}
