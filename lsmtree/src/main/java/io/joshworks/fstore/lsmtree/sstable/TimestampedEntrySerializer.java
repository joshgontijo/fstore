package io.joshworks.fstore.lsmtree.sstable;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;

public class TimestampedEntrySerializer<K extends Comparable<K>, V> implements Serializer<Entry<K, V>> {

    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;

    public TimestampedEntrySerializer(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }

    //Order must be key, timestamp and value
    @Override
    public void writeTo(Entry<K, V> data, ByteBuffer dst) {
        keySerializer.writeTo(data.key, dst);
        dst.putLong(data.timestamp);
        if (data.value != null) {
            valueSerializer.writeTo(data.value, dst);
        }
    }

    @Override
    public Entry<K, V> fromBytes(ByteBuffer buffer) {
        K k = keySerializer.fromBytes(buffer);
        long timestamp = buffer.getLong();
        V v = buffer.hasRemaining() ? valueSerializer.fromBytes(buffer) : null;
        return Entry.of(timestamp, k, v);
    }
}
