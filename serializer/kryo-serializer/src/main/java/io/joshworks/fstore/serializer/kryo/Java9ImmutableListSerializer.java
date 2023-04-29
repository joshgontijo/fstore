package io.joshworks.fstore.serializer.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.util.ArrayList;
import java.util.List;

class Java9ImmutableListSerializer extends Serializer<List<Object>> {

    private static final boolean DOES_NOT_ACCEPT_NULL = true;
    private static final boolean IMMUTABLE = true;

    public Java9ImmutableListSerializer() {
        super(DOES_NOT_ACCEPT_NULL, IMMUTABLE);
    }

    /**
     * for the several ImmutableMap related classes.
     *
     * @param kryo the {@link Kryo} instance to set the serializer on
     */
    public static void registerSerializers(final Kryo kryo) {

        final Java9ImmutableListSerializer serializer = new Java9ImmutableListSerializer();


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


        kryo.register(List.of().getClass(), serializer);
        kryo.register(List.of(value0).getClass(), serializer);
        kryo.register(List.of(value0, value1).getClass(), serializer);
        kryo.register(List.of(value0, value1, value2, value3).getClass(), serializer);
        kryo.register(List.of(value0, value1, value2, value3, value4, value5).getClass(), serializer);
        kryo.register(List.of(value0, value1, value2, value3, value4, value5, value6, value7).getClass(), serializer);
        kryo.register(List.of(value0, value1, value2, value3, value4, value5, value6, value7, value8, value9).getClass(), serializer);
        kryo.register(List.of(value0, value1, value2, value3, value4, value5, value6, value7, value8, value9, value10).getClass(), serializer);
        kryo.register(List.of(value0, value1, value2, value3, value4, value5, value6, value7, value8, value9, value10, value11).getClass(), serializer);
        kryo.register(List.copyOf(new ArrayList<>()).getClass(), serializer);

    }

    @Override
    public void write(Kryo kryo, Output output, List<Object> immutableList) {
        kryo.writeObject(output, new ArrayList<>(immutableList));
    }

    @Override
    public List<Object> read(Kryo kryo, Input input, Class<List<Object>> type) {
        List<Object> list = kryo.readObject(input, ArrayList.class);
        return List.copyOf(list);
    }
}