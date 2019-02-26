package io.joshworks.fstore.serializer.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

class Java9ImmutableListSerializer extends Serializer<List<Object>> {

    private static final boolean DOES_NOT_ACCEPT_NULL = true;
    private static final boolean IMMUTABLE = true;

    public Java9ImmutableListSerializer() {
        super(DOES_NOT_ACCEPT_NULL, IMMUTABLE);
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

    /**
     * for the several ImmutableMap related classes.
     *
     * @param kryo the {@link Kryo} instance to set the serializer on
     */
    public static void registerSerializers(final Kryo kryo) {

       final Java9ImmutableListSerializer serializer = new Java9ImmutableListSerializer();

        Object key1 = new Object();
        Object key2 = new Object();
        Object key3 = new Object();
        Object key4 = new Object();
        Object key5 = new Object();
        Object key6 = new Object();
        Object key7 = new Object();
        Object key8 = new Object();
        Object key9 = new Object();
        Object key10 = new Object();
        Object value = new Object();


        kryo.register(List.of(key1, value).getClass(), serializer);
        kryo.register(List.of(key1, value, key2, value).getClass(), serializer);
        kryo.register(List.of(key1, value, key2, value, key3, value).getClass(), serializer);
        kryo.register(List.of(key1, value, key2, value, key3, value, key4, value).getClass(), serializer);
        kryo.register(List.of(key1, value, key2, value, key3, value, key4, value, key5, value).getClass(), serializer);
        kryo.register(List.of(key1, value, key2, value, key3, value, key4, value, key5, value, key6, value).getClass(), serializer);
        kryo.register(List.of(key1, value, key2, value, key3, value, key4, value, key5, value, key6, value, key7, value).getClass(), serializer);
        kryo.register(List.of(key1, value, key2, value, key3, value, key4, value, key5, value, key6, value, key7, value, key8, value).getClass(), serializer);
        kryo.register(List.of(key1, value, key2, value, key3, value, key4, value, key5, value, key6, value, key7, value, key8, value, key9, value).getClass(), serializer);
        kryo.register(List.of(key1, value, key2, value, key3, value, key4, value, key5, value, key6, value, key7, value, key8, value, key9, value, key10, value).getClass(), serializer);

    }
}