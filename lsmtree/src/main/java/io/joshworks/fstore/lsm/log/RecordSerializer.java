package io.joshworks.fstore.lsm.log;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.lsm.EntryType;

import java.nio.ByteBuffer;

public class RecordSerializer<K, V> implements Serializer<Record<K, V>> {

    private static final ByteBuffer EMPTY = ByteBuffer.allocate(0);
    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;

    public RecordSerializer(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }

    @Override
    public ByteBuffer toBytes(Record<K, V> data) {
        ByteBuffer key = data.key != null ? keySerializer.toBytes(data.key) : EMPTY;
        ByteBuffer value = data.key != null ? valueSerializer.toBytes(data.value) : EMPTY;

        ByteBuffer rec = ByteBuffer.allocate(Integer.BYTES + key.limit() + value.limit());

        return rec.putInt(data.type.code).put(key).put(value).flip();
    }

    @Override
    public void writeTo(Record<K, V> data, ByteBuffer dest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Record<K, V> fromBytes(ByteBuffer buffer) {
        int code = buffer.getInt();
        if (code == EntryType.ADD.code) {
            K k = keySerializer.fromBytes(buffer);
            V v = valueSerializer.fromBytes(buffer);
            return Record.add(k, v);
        }
        if (code == EntryType.DELETE.code) {
            K k = keySerializer.fromBytes(buffer);
            return Record.delete(k);
        }
        if (code == EntryType.MEM_FLUSHED.code) {
            return Record.memFlushed();
        }
        throw new IllegalStateException("Unknown record type: " + code);
    }
}
