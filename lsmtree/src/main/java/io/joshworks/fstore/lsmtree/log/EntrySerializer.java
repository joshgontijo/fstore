package io.joshworks.fstore.lsmtree.log;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.lsmtree.EntryType;

import java.nio.ByteBuffer;

public class EntrySerializer<K extends Comparable<K>, V> implements Serializer<LogEntry<K, V>> {

    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;

    public EntrySerializer(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }

    @Override
    public void writeTo(LogEntry<K, V> data, ByteBuffer dst) {
        dst.putInt(data.type.code);
        keySerializer.writeTo(data.key, dst);
        valueSerializer.writeTo(data.value, dst);
    }

    @Override
    public LogEntry<K, V> fromBytes(ByteBuffer buffer) {
        int type = buffer.getInt();
        if (EntryType.ADD.code == type) {
            K k = keySerializer.fromBytes(buffer);
            V v = valueSerializer.fromBytes(buffer);
            return LogEntry.add(k, v);
        }
        if (EntryType.DELETE.code == type) {
            K k = keySerializer.fromBytes(buffer);
            return LogEntry.delete(k);
        }
        throw new IllegalStateException("Unknown record type: " + type);
    }
}
