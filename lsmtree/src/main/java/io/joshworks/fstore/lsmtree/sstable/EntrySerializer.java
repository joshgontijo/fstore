package io.joshworks.fstore.lsmtree.sstable;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;

public class EntrySerializer<K extends Comparable<K>, V> implements Serializer<Entry<K, V>> {

    private static final ByteBuffer EMPTY = ByteBuffer.allocate(0);
    public static final int KEY_START_POS = 1;

    private final Serializer<K> keySerializer;
    private final Serializer<V> valueSerializer;

    public EntrySerializer(Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
    }

    @Override
    public void writeTo(Entry<K, V> data, ByteBuffer dst) {
        dst.put((byte) (data.deletion ? 1 : 0));
        keySerializer.writeTo(data.key, dst);
        if (data.value != null) {
            valueSerializer.writeTo(data.value, dst);
        }
    }

    @Override
    public Entry<K, V> fromBytes(ByteBuffer buffer) {
        boolean deletion = ((int) buffer.get()) == 1;
        if (deletion) {
            K k = keySerializer.fromBytes(buffer);
            return Entry.delete(k);
        }
        K k = keySerializer.fromBytes(buffer);
        V v = valueSerializer.fromBytes(buffer);
        return Entry.add(k, v);
    }
}
