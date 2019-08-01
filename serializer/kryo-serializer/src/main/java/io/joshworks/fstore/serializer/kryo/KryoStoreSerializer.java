package io.joshworks.fstore.serializer.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInputStream;
import com.esotericsoftware.kryo.io.ByteBufferOutputStream;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.DefaultSerializers;
import de.javakaffee.kryoserializers.ArraysAsListSerializer;
import de.javakaffee.kryoserializers.GregorianCalendarSerializer;
import de.javakaffee.kryoserializers.JdkProxySerializer;
import de.javakaffee.kryoserializers.SynchronizedCollectionsSerializer;
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;
import io.joshworks.fstore.core.Serializer;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationHandler;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;


public class KryoStoreSerializer<T> implements Serializer<T> {

    private static final ThreadLocal<Kryo> localKryo = ThreadLocal.withInitial(KryoStoreSerializer::newKryoInstance);
    private final Class<T> mainType;

    private KryoStoreSerializer(Class<T> mainType) {
        this.mainType = mainType;
    }

    private static Kryo newKryoInstance() {
        Kryo kryo = new Kryo();
        kryo.setInstantiatorStrategy(new Kryo.DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
        kryo.register(Arrays.asList("").getClass(), new ArraysAsListSerializer());
        kryo.register(Collections.emptyList().getClass(), new DefaultSerializers.CollectionsEmptyListSerializer());
        kryo.register(Collections.emptyMap().getClass(), new DefaultSerializers.CollectionsEmptyMapSerializer());
        kryo.register(Collections.emptySet().getClass(), new DefaultSerializers.CollectionsEmptySetSerializer());
        kryo.register(Collections.singletonList("").getClass(), new DefaultSerializers.CollectionsSingletonListSerializer());
        kryo.register(Collections.singleton("").getClass(), new DefaultSerializers.CollectionsSingletonSetSerializer());
        kryo.register(Collections.singletonMap("", "").getClass(), new DefaultSerializers.CollectionsSingletonMapSerializer());
        kryo.register(GregorianCalendar.class, new GregorianCalendarSerializer());
        kryo.register(InvocationHandler.class, new JdkProxySerializer());
        UnmodifiableCollectionsSerializer.registerSerializers(kryo);
        SynchronizedCollectionsSerializer.registerSerializers(kryo);
        Java9ImmutableMapSerializer.registerSerializers(kryo);
        return kryo;
    }

    public static <K, V> KryoStoreSerializer<Map<K, V>> mapOf(Class<K> keytype, Class<V> valueType) {
        return (KryoStoreSerializer<Map<K, V>>) untyped(keytype, valueType);
    }

    public static <T> KryoStoreSerializer<List<T>> listOf(Class<T> type) {
        return (KryoStoreSerializer<List<T>>) untyped(type);
    }

    public static <T> KryoStoreSerializer<Set<T>> setOf(Class<T> type) {
        return (KryoStoreSerializer<Set<T>>) untyped(type);
    }

    public static <T> KryoStoreSerializer<Collection<T>> collectionOf(Class<T> type) {
        return (KryoStoreSerializer<Collection<T>>) untyped(type);
    }

    public static byte[] serialize(Object data) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        serializeInternal(data, null, baos);
        return baos.toByteArray();
    }

    public static byte[] serialize(Object data, Class<?> type) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        serializeInternal(data, type, baos);
        return baos.toByteArray();
    }

    public static void serialize(Object data, ByteBuffer buffer) {
        serialize(data, buffer, null);
    }

    public static void serialize(Object data, ByteBuffer buffer, Class<?> type) {
        OutputStream baos = new ByteBufferOutputStream(buffer);
        serializeInternal(data, type, baos);
    }

    private static void serializeInternal(Object data, Class<?> type, OutputStream baos) {
        Kryo kryo = localKryo.get();
        try (Output output = new Output(baos)) {
            if (type != null) {
                kryo.writeObject(output, data);
            }
            kryo.writeClassAndObject(output, data);
        }
    }

    public static <T> T deserialize(byte[] data) {
        return deserialize(data, null);
    }

    public static <T> T deserialize(byte[] data, Class<T> type) {
        Kryo kryo = localKryo.get();
        InputStream inputStream = new ByteArrayInputStream(data);
        try (Input input = new Input(inputStream)) {
            if (type != null) {
                return kryo.readObject(input, type);
            }
            return (T) kryo.readClassAndObject(input);
        }
    }

    public static <T> T deserialize(ByteBuffer data) {
        return deserialize(data, null);
    }

    public static <T> T deserialize(ByteBuffer data, Class<T> type) {
        Kryo kryo = localKryo.get();
        InputStream inputStream = new ByteBufferInputStream(data);
        try (Input input = new Input(inputStream)) {
            if (type != null) {
                return kryo.readObject(input, type);
            }
            return (T) kryo.readClassAndObject(input);
        }
    }

    public static <T> KryoStoreSerializer<T> of(Class<T> mainType, Class... types) {
        requireNonNull(mainType, "Class type must be provided");
        Kryo kryo = localKryo.get();
        if (types != null) {
            for (Class type : types) {
                kryo.register(type);
            }
        }
        kryo.register(mainType);

        return new KryoStoreSerializer<>(mainType);
    }

    public static KryoStoreSerializer<?> untyped(Class... types) {
        if (types != null) {
            Kryo kryo = localKryo.get();
            for (Class type : types) {
                kryo.register(type);
            }
        }
        return new KryoStoreSerializer<>(null);
    }

    @Override
    public void writeTo(T data, ByteBuffer dst) {
        ByteBufferOutputStream baos = new ByteBufferOutputStream(dst);
        serializeInternal(data, mainType, baos);
    }

    @Override
    public T fromBytes(ByteBuffer data) {
        Kryo kryo = localKryo.get();
        InputStream inputStream = new ByteBufferInputStream(data);
        try (Input input = new Input(inputStream)) {
            if (mainType != null) {
                return kryo.readObject(input, mainType);
            }
            return (T) kryo.readClassAndObject(input);
        }
    }
}
