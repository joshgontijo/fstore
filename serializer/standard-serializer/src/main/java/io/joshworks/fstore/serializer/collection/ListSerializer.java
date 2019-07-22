package io.joshworks.fstore.serializer.collection;

import io.joshworks.fstore.core.Serializer;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public class ListSerializer<V> extends CollectionSerializer<V, List<V>> {

    public ListSerializer(Serializer<V> valueSerializer) {
        super(valueSerializer, ArrayList::new);
    }

    public ListSerializer(Serializer<V> valueSerializer, Supplier<List<V>> instanceSupplier) {
        super(valueSerializer, instanceSupplier);
    }
}
