package io.joshworks.fstore.serializer.kryo;

import com.esotericsoftware.kryo.Kryo;
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

import java.io.ByteArrayOutputStream;
import java.lang.reflect.InvocationHandler;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.GregorianCalendar;


public class KryoStoreSerializer<T> implements Serializer<T> {

    static private final ThreadLocal<Kryo> localKryo = ThreadLocal.withInitial(KryoStoreSerializer::newKryoInstance);

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

    public static <T> KryoStoreSerializer<T> of(Class... types) {
        if (types != null) {
            Kryo kryo = localKryo.get();
            for (Class type : types) {
                kryo.register(type);
            }
        }
        return new KryoStoreSerializer<>();
    }

    @Override
    public ByteBuffer toBytes(T data) {
        byte[] bytes = write(data);
        return ByteBuffer.wrap(bytes);
    }

    private byte[] write(T data) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Kryo kryo = localKryo.get();
        try (Output output = new Output(baos)) {
            kryo.writeClassAndObject(output, data);
        }
        return baos.toByteArray();
    }

    @Override
    public void writeTo(T data, ByteBuffer dest) {
        byte[] bytes = write(data);
        dest.put(bytes);
    }

    @Override
    public T fromBytes(ByteBuffer data) {
        Kryo kryo = localKryo.get();
        Input input = new Input(data.array());
        return (T) kryo.readClassAndObject(input);
    }


}
