package io.joshworks.fstore.serializer.collection;

import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.function.Supplier;

public class CollectionSerializer<V, T extends Collection<V>> implements Serializer<T> {

    private final Serializer<V> valueSerializer;
    private final Supplier<T> instanceSupplier;

    public CollectionSerializer(Serializer<V> valueSerializer, Supplier<T> instanceSupplier) {
        this.valueSerializer = valueSerializer;
        this.instanceSupplier = instanceSupplier;
    }

    @Override
    public void writeTo(T data, ByteBuffer dst) {
        dst.putInt(data.size());
        for (V entry : data) {
            valueSerializer.writeTo(entry, dst);
        }
    }

    @Override
    public T fromBytes(ByteBuffer buffer) {
        int size = buffer.getInt();

        T list = instanceSupplier.get();

        for (int i = 0; i < size; i++) {
            V value = valueSerializer.fromBytes(buffer);
            list.add(value);
        }
        return list;
    }

}
