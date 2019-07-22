package io.joshworks.fstore.serializer.collection;

import io.joshworks.fstore.core.Serializer;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;

public class SetSerializer<V> extends CollectionSerializer<V, Set<V>> {

    public SetSerializer(Serializer<V> valueSerializer) {
        super(valueSerializer, HashSet::new);
    }

    public SetSerializer(Serializer<V> valueSerializer, Supplier<Set<V>> instanceSupplier) {
        super(valueSerializer, instanceSupplier);
    }
}
