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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class is NOT thread safe
 */
public class KryoStoreSerializer implements Serializer<Object> {

    private static final Set<Class> registered = Collections.newSetFromMap(new ConcurrentHashMap<>());
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
        for (Class type : registered) {
            kryo.register(type);
        }
        return kryo;
    }

    public void register(Class type) {
        registered.add(type);
        localKryo.get().register(type);
    }

    @Override
    public ByteBuffer toBytes(Object data) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try(Output output = new Output(baos)) {
            localKryo.get().writeClassAndObject(output, data);
        }
        return ByteBuffer.wrap(baos.toByteArray());
    }

    @Override
    public void writeTo(Object data, ByteBuffer dest) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try(Output output = new Output(baos)) {
            localKryo.get().writeClassAndObject(output, data);
        }
        dest.put(baos.toByteArray());
    }

    @Override
    public Object fromBytes(ByteBuffer data) {
        return localKryo.get().readClassAndObject(new Input(data.array()));
    }


}
