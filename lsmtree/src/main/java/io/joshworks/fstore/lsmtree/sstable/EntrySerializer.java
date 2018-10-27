package io.joshworks.fstore.lsmtree.sstable;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.lsmtree.EntryType;

import java.nio.ByteBuffer;

public class EntrySerializer<K extends Comparable<K>, V> implements Serializer<Entry<K, V>> {

    private static final ByteBuffer EMPTY = ByteBuffer.allocate(0);

    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;

    public EntrySerializer(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }

    @Override
    public ByteBuffer toBytes(Entry<K, V> data) {
        ByteBuffer key = keySerializer.toBytes(data.key);
        ByteBuffer val = EntryType.ADD.equals(data.type) ? valueSerializer.toBytes(data.value) : EMPTY;
        ByteBuffer bb = ByteBuffer.allocate(Integer.BYTES + key.limit() + val.limit());
        return bb.putInt(data.type.code).put(key).put(val).flip();
    }

    @Override
    public void writeTo(Entry<K, V> data, ByteBuffer dest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Entry<K, V> fromBytes(ByteBuffer buffer) {
        int type = buffer.getInt();
        if (EntryType.ADD.code == type) {
            K k = keySerializer.fromBytes(buffer);
            V v = valueSerializer.fromBytes(buffer);
            return Entry.add(k, v);
        }
        if (EntryType.DELETE.code == type) {
            K k = keySerializer.fromBytes(buffer);
            return Entry.delete(k);
        }
        throw new IllegalStateException("Unknown record type: " + type);
    }
}
