package io.joshworks.fstore.serializer.collection;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;
import java.util.AbstractMap;
import java.util.Map;

public class EntrySerializer<K, V> implements Serializer<Map.Entry<K, V>> {

    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;

    public EntrySerializer(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }

    @Override
    public void writeTo(Map.Entry<K, V> data, ByteBuffer dst) {
        keySerializer.writeTo(data.getKey(), dst);
        valueSerializer.writeTo(data.getValue(), dst);
    }

    @Override
    public Map.Entry<K, V> fromBytes(ByteBuffer buffer) {
        K key = keySerializer.fromBytes(buffer);
        V value = valueSerializer.fromBytes(buffer);
        return new AbstractMap.SimpleEntry<>(key, value);

    }
}
