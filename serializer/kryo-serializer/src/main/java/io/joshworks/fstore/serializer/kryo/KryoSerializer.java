//package io.joshworks.fstore.serializer.kryo;
//
//import com.esotericsoftware.kryo.Kryo;
//import com.esotericsoftware.kryo.io.Input;
//import com.esotericsoftware.kryo.io.Output;
//import com.esotericsoftware.kryo.serializers.DefaultSerializers;
//import com.esotericsoftware.kryo.serializers.MapSerializer;
//import de.javakaffee.kryoserializers.ArraysAsListSerializer;
//import de.javakaffee.kryoserializers.GregorianCalendarSerializer;
//import de.javakaffee.kryoserializers.JdkProxySerializer;
//import de.javakaffee.kryoserializers.SynchronizedCollectionsSerializer;
//import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;
//import io.joshworks.fstore.core.Serializer;
//import org.objenesis.strategy.StdInstantiatorStrategy;
//
//import java.io.ByteArrayOutputStream;
//import java.lang.reflect.InvocationHandler;
//import java.nio.ByteBuffer;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.Collections;
//import java.util.GregorianCalendar;
//import java.util.HashMap;
//import java.util.LinkedHashMap;
//import java.util.LinkedList;
//import java.util.concurrent.ConcurrentHashMap;
//
//import static java.util.Objects.requireNonNull;
//
//public class KryoSerializer<T> implements Serializer<T> {
//
//    private final Kryo kryo;
//    private final Class<? super T> type;
//
//    private KryoSerializer(Kryo kryo, Class<T> type) {
//        this.kryo = kryo;
//        this.type = type;
//    }
//
//    private static Kryo newInstance() {
//        Kryo kryo = new Kryo();
//        kryo.register(HashMap.class);
//        kryo.register(LinkedHashMap.class, new MapSerializer());
//        kryo.register(LinkedHashMap.class, new MapSerializer());
//        kryo.register(ConcurrentHashMap.class, new MapSerializer());
//        kryo.register(ArrayList.class);
//        kryo.register(LinkedList.class);
//        kryo.setInstantiatorStrategy(new Kryo.DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
//        kryo.register(Arrays.asList("").getClass(), new ArraysAsListSerializer());
//        kryo.register(Collections.emptyList().getClass(), new DefaultSerializers.CollectionsEmptyListSerializer());
//        kryo.register(Collections.emptyMap().getClass(), new DefaultSerializers.CollectionsEmptyMapSerializer());
//        kryo.register(Collections.emptySet().getClass(), new DefaultSerializers.CollectionsEmptySetSerializer());
//        kryo.register(Collections.singletonList("").getClass(), new DefaultSerializers.CollectionsSingletonListSerializer());
//        kryo.register(Collections.singleton("").getClass(), new DefaultSerializers.CollectionsSingletonSetSerializer());
//        kryo.register(Collections.singletonMap("", "").getClass(), new DefaultSerializers.CollectionsSingletonMapSerializer());
//        kryo.register(GregorianCalendar.class, new GregorianCalendarSerializer());
//        kryo.register(InvocationHandler.class, new JdkProxySerializer());
//        UnmodifiableCollectionsSerializer.registerSerializers(kryo);
//        SynchronizedCollectionsSerializer.registerSerializers(kryo);
//        Java9ImmutableMapSerializer.registerSerializers(kryo);
//        Java9ImmutableListSerializer.registerSerializers(kryo);
//        Java9ImmutableSetSerializer.registerSerializers(kryo);
//        return kryo;
//    }
//
//    public static <T> KryoSerializer<T> of(Class<T> type) {
//        requireNonNull(type, "Type must be provided");
//        Kryo kryo = newInstance();
//        kryo.register(type);
//        return new KryoSerializer<>(kryo, type);
//    }
//
//    public static <T> KryoSerializer<T> untyped() {
//        Kryo kryo = newInstance();
//        return new KryoSerializer<>(kryo, null);
//    }
//
//    public KryoSerializer<T> register(Class type) {
//        kryo.register(type);
//        return this;
//    }
//
//    public KryoSerializer<T> register(Class type, com.esotericsoftware.kryo.Serializer serializer) {
//        kryo.register(type, serializer);
//        return this;
//    }
//
//    @Override
//    public ByteBuffer toBytes(T data) {
//        ByteArrayOutputStream baos = new ByteArrayOutputStream();
//        writeTo(data, baos);
//        return ByteBuffer.wrap(baos.toByteArray());
//    }
//
//
//    @Override
//    public void writeTo(T data, ByteBuffer dest) {
//        ByteArrayOutputStream baos = new ByteArrayOutputStream();
//        writeTo(data, baos);
//        dest.put(baos.toByteArray());
//    }
//
//    @Override
//    public T fromBytes(ByteBuffer data) {
//        Input input = new Input(data.array());
//        if (type == null) {
//            return (T) kryo.readClassAndObject(input);
//        }
//        return (T) kryo.readObject(input, type);
//    }
//
//    private void writeTo(T data, ByteArrayOutputStream baos) {
//        try (Output output = new Output(baos)) {
//            if (type == null) {
//                kryo.writeClassAndObject(output, data);
//            } else {
//                kryo.writeObject(output, data);
//            }
//        }
//    }
//
//}
