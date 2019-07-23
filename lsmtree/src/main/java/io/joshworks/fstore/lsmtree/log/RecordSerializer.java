package io.joshworks.fstore.lsmtree.log;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.lsmtree.EntryType;

import java.nio.ByteBuffer;

public class RecordSerializer<K, V> implements Serializer<LogRecord<K, V>> {

    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;

    public RecordSerializer(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }

    @Override
    public void writeTo(LogRecord<K, V> data, ByteBuffer dst) {
        dst.putShort(data.type.code);
        if (data.key != null) {
            keySerializer.writeTo(data.key, dst);
        }
        if (data.value != null) {
            valueSerializer.writeTo(data.value, dst);
        }
    }

    @Override
    public LogRecord<K, V> fromBytes(ByteBuffer buffer) {
        short code = buffer.getShort();
        if (code == EntryType.ADD.code) {
            K k = keySerializer.fromBytes(buffer);
            V v = valueSerializer.fromBytes(buffer);
            return LogRecord.add(k, v);
        }
        if (code == EntryType.DELETE.code) {
            K k = keySerializer.fromBytes(buffer);
            return LogRecord.delete(k);
        }
        if (code == EntryType.MEM_FLUSHED.code) {
            return LogRecord.memFlushed();
        }
        throw new IllegalStateException("Unknown record type: " + code);
    }
}
