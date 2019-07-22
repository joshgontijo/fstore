package io.joshworks.fstore.serializer.collection;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.function.Supplier;

public class MapSerializer<K, V> implements Serializer<Map<K, V>> {

    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;
    private final Supplier<Map<K, V>> instanceSupplier;

    public MapSerializer(Serializer<K> keySerializer, Serializer<V> valueSerializer, Supplier<Map<K, V>> instanceSupplier) {
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.instanceSupplier = instanceSupplier;
    }

    @Override
    public void writeTo(Map<K, V> data, ByteBuffer dst) {
        dst.putInt(data.size());
        for (Map.Entry<K, V> entry : data.entrySet()) {
            keySerializer.writeTo(entry.getKey(), dst);
            valueSerializer.writeTo(entry.getValue(), dst);
        }
    }

    @Override
    public Map<K, V> fromBytes(ByteBuffer buffer) {
        Map<K, V> kvMap = instanceSupplier.get();

        int size = buffer.getInt();
        for (int i = 0; i < size; i++) {
            K key = keySerializer.fromBytes(buffer);
            V value = valueSerializer.fromBytes(buffer);
            kvMap.put(key, value);
        }
        return kvMap;
    }
}
