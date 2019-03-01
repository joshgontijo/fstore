package io.joshworks.fstore.serializer.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

class Java9ImmutableSetSerializer extends Serializer<Set<Object>> {

    private static final boolean DOES_NOT_ACCEPT_NULL = true;
    private static final boolean IMMUTABLE = true;

    public Java9ImmutableSetSerializer() {
        super(DOES_NOT_ACCEPT_NULL, IMMUTABLE);
    }


    @Override
    public void write(Kryo kryo, Output output, Set<Object> immutableSet) {
        kryo.writeObject(output, new LinkedHashSet<>(immutableSet));
    }

    @Override
    public Set<Object> read(Kryo kryo, Input input, Class<Set<Object>> type) {
        Set<Object> set = kryo.readObject(input, LinkedHashSet.class);
        return Set.copyOf(set);
    }

    /**
     * for the several ImmutableMap related classes.
     *
     * @param kryo the {@link Kryo} instance to set the serializer on
     */
    public static void registerSerializers(final Kryo kryo) {

       final Java9ImmutableSetSerializer serializer = new Java9ImmutableSetSerializer();

        Object value0 = new Object();
        Object value1 = new Object();
        Object value2 = new Object();
        Object value3 = new Object();
        Object value4 = new Object();
        Object value5 = new Object();
        Object value6 = new Object();
        Object value7 = new Object();
        Object value8 = new Object();
        Object value9 = new Object();
        Object value10 = new Object();
        Object value11 = new Object();


        kryo.register(Set.of().getClass(), serializer);
        kryo.register(Set.of(value0).getClass(), serializer);
        kryo.register(Set.of(value0, value1).getClass(), serializer);
        kryo.register(Set.of(value0, value1, value2, value3).getClass(), serializer);
        kryo.register(Set.of(value0, value1, value2, value3, value4, value5).getClass(), serializer);
        kryo.register(Set.of(value0, value1, value2, value3, value4, value5, value6, value7).getClass(), serializer);
        kryo.register(Set.of(value0, value1, value2, value3, value4, value5, value6, value7, value8, value9).getClass(), serializer);
        kryo.register(Set.of(value0, value1, value2, value3, value4, value5, value6, value7, value8, value9, value10).getClass(), serializer);
        kryo.register(Set.of(value0, value1, value2, value3, value4, value5, value6, value7, value8, value9, value10, value11).getClass(), serializer);
        kryo.register(Set.copyOf(new ArrayList<>()).getClass(), serializer);

    }
}