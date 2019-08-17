package io.joshworks.fstore.client.tcp;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryonet.Client;
import com.esotericsoftware.kryonet.Connection;
import com.esotericsoftware.kryonet.Listener;
import io.joshworks.fstore.es.shared.EventId;
import io.joshworks.fstore.es.shared.EventRecord;
import io.joshworks.fstore.es.shared.NodeInfo;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static io.joshworks.fstore.es.shared.tcp.Message.NO_RESP;

public class TcpClient {

    private static final Logger logger = LoggerFactory.getLogger(TcpClient.class);

    private final Map<Long, Response> responseTable = new ConcurrentHashMap<>();
    //    private final Map<Class, Consumer> handlers = new ConcurrentHashMap<>();
    private final AtomicLong reqids = new AtomicLong();
    private final Client client;

    private static final Consumer<?> NO_OP = msg -> logger.warn("No handler for message {}", msg.getClass().getSimpleName());

    private final NodeInfo nodeInfo;

    public TcpClient(NodeInfo nodeInfo) {
        this.nodeInfo = nodeInfo;
        client = new Client(8192 * 10, 8192 * 10);
        setupSerialization(client.getKryo());
        client.addListener(new Listener.ThreadedListener(new EventListener()));
    }

    private static void setupSerialization(Kryo kryo) {
//        kryo.setInstantiatorStrategy(new Kryo.DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));

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

        kryo.register(byte[].class);
//        kryo.register(Arrays.asList("").getClass(), new ArraysAsListSerializer());
//        kryo.register(Arrays.asList("").getClass(), new ArraysAsListSerializer());
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

        kryo.register(ArrayList.class);
        kryo.register(HashSet.class);

    }

//    public <T extends Message> void register(Class<T> type, Consumer<T> func) {
//        if (client.isConnected()) {
//            throw new IllegalStateException("Client already connected");
//        }
//        client.getKryo().register(type);
//        handlers.put(type, func);
//    }

    public void connect(String host, int port) {
        try {
            client.start();
            client.connect(5000, host, port);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void sendAsync(Message message) {
        client.sendTCP(message);
    }

    public <T extends Message> Response<T> send(Message message) {
        message.id = reqids.incrementAndGet();
        Response<T> response = new Response<>(message.id, responseTable::remove);
        responseTable.put(message.id, response);

        client.sendTCP(message);
        return response;
    }

    private final class EventListener extends Listener {
        @Override
        public void connected(Connection connection) {
            logger.info("Connected to " + connection.getRemoteAddressTCP());
        }

        @Override
        public void disconnected(Connection connection) {
            logger.info("Disconnected from " + connection.getRemoteAddressTCP());
        }

        @Override
        public void received(Connection connection, Object object) {
            if (object instanceof Message) {
                Message msg = (Message) object;
                if (msg.id == NO_RESP) {
                    logger.warn("Received event client was not expecting");
                    return;
                }
                responseTable.remove(msg.id).complete(msg);
            }
            //assuming server will never ask for a response from the client
        }

    }

}
