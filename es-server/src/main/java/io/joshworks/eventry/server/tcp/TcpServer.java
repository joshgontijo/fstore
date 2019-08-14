package io.joshworks.eventry.server.tcp;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.serializers.DefaultSerializers;
import com.esotericsoftware.kryonet.Listener;
import com.esotericsoftware.kryonet.Server;
import de.javakaffee.kryoserializers.ArraysAsListSerializer;
import de.javakaffee.kryoserializers.GregorianCalendarSerializer;
import de.javakaffee.kryoserializers.JdkProxySerializer;
import de.javakaffee.kryoserializers.SynchronizedCollectionsSerializer;
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;
import io.joshworks.eventry.server.ClusterStore;
import io.joshworks.eventry.server.subscription.polling.LocalPollingSubscription;
import io.joshworks.fstore.es.shared.EventId;
import io.joshworks.fstore.es.shared.EventRecord;
import io.joshworks.fstore.es.shared.tcp.Ack;
import io.joshworks.fstore.es.shared.tcp.Append;
import io.joshworks.fstore.es.shared.tcp.CreateStream;
import io.joshworks.fstore.es.shared.tcp.CreateSubscription;
import io.joshworks.fstore.es.shared.tcp.ErrorMessage;
import io.joshworks.fstore.es.shared.tcp.EventCreated;
import io.joshworks.fstore.es.shared.tcp.EventData;
import io.joshworks.fstore.es.shared.tcp.EventsData;
import io.joshworks.fstore.es.shared.tcp.GetEvent;
import io.joshworks.fstore.es.shared.tcp.Message;
import io.joshworks.fstore.es.shared.tcp.SubscriptionCreated;
import io.joshworks.fstore.es.shared.tcp.SubscriptionIteratorNext;
import io.joshworks.fstore.serializer.kryo.Java9ImmutableMapSerializer;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.Closeable;
import java.lang.reflect.InvocationHandler;
import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TcpServer implements Closeable {

    private final Server server;

    private final ExecutorService worker;

    private TcpServer(Server server, ExecutorService worker) {
        this.server = server;
        this.worker = worker;
    }

    public static TcpServer start(ClusterStore store, LocalPollingSubscription subscription,  InetSocketAddress bindAddress) {
        try {
            ExecutorService worker = Executors.newFixedThreadPool(10);
            Server server = new Server();
            setupSerialization(server.getKryo());
            server.addListener(new Listener.ThreadedListener(new TcpEventHandler(store, subscription), worker));
            server.addListener(new Listener.ThreadedListener(new Listener.ReflectionListener(), worker));
            server.start();
            server.bind(bindAddress, null);
            System.out.println("STARTED TCP SERVER ON: " + bindAddress);
            return new TcpServer(server, worker);
        } catch (Exception e) {
            throw new RuntimeException("Could not start TCP server", e);
        }
    }

    private static void setupSerialization(Kryo kryo) {
        kryo.setInstantiatorStrategy(new Kryo.DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
        kryo.register(byte[].class);
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

        kryo.register(Message.class);
        kryo.register(EventRecord.class);
        kryo.register(EventCreated.class);
        kryo.register(EventId.class);
        kryo.register(Ack.class);
        kryo.register(Append.class);
        kryo.register(CreateStream.class);
        kryo.register(ErrorMessage.class);
        kryo.register(EventData.class);
        kryo.register(EventsData.class);
        kryo.register(GetEvent.class);
        kryo.register(CreateSubscription.class);
        kryo.register(SubscriptionCreated.class);
        kryo.register(SubscriptionIteratorNext.class);

    }

    @Override
    public void close()  {
        server.close();
    }
}
