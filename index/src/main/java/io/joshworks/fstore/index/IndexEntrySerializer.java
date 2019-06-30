package io.joshworks.fstore.index;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;

public class IndexEntrySerializer<K extends Comparable<K>> implements Serializer<IndexEntry<K>> {

    private final Serializer<K> indexEntrySerializer;

    public IndexEntrySerializer(Serializer<K> indexEntrySerializer) {
        this.indexEntrySerializer = indexEntrySerializer;
    }

    @Override
    public ByteBuffer toBytes(IndexEntry<K> data) {
        ByteBuffer keyData = indexEntrySerializer.toBytes(data.key);
        ByteBuffer bb = ByteBuffer.allocate(keyData.limit() + Long.BYTES);
        bb.put(keyData);
        bb.putLong(data.position);
        return bb.flip();
    }

    @Override
    public void writeTo(IndexEntry<K> data, ByteBuffer dest) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IndexEntry<K> fromBytes(ByteBuffer buffer) {
        K key = indexEntrySerializer.fromBytes(buffer);
        long position = buffer.getLong();
        return new IndexEntry<>(key, position);
    }
}
