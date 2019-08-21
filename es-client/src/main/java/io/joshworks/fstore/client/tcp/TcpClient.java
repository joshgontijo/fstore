package io.joshworks.fstore.client.tcp;

import io.joshworks.eventry.network.tcp.EventHandler;
import io.joshworks.eventry.network.tcp.TcpConnection;
import io.joshworks.eventry.network.tcp.TcpEventClient;
import io.joshworks.fstore.es.shared.NodeInfo;
import io.joshworks.fstore.es.shared.tcp.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xnio.Options;

import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static io.joshworks.fstore.es.shared.tcp.Message.NO_RESP;

public class TcpClient {

    private static final Logger logger = LoggerFactory.getLogger(TcpClient.class);

    private final Map<Long, Response> responseTable = new ConcurrentHashMap<>();
    //    private final Map<Class, Consumer> handlers = new ConcurrentHashMap<>();
    private final AtomicLong reqids = new AtomicLong();
    private TcpConnection client;

    private static final Consumer<?> NO_OP = msg -> logger.warn("No handler for message {}", msg.getClass().getSimpleName());

    private final NodeInfo nodeInfo;

    public TcpClient(NodeInfo nodeInfo) {
        this.nodeInfo = nodeInfo;
    }

//    private static void setupSerialization(Kryo kryo) {
//        kryo.register(Message.class);
//        kryo.register(EventRecord.class);
//        kryo.register(EventCreated.class);
//        kryo.register(EventId.class);
//        kryo.register(Ack.class);
//        kryo.register(Append.class);
//        kryo.register(CreateStream.class);
//        kryo.register(ErrorMessage.class);
//        kryo.register(EventData.class);
//        kryo.register(EventsData.class);
//        kryo.register(GetEvent.class);
//        kryo.register(CreateSubscription.class);
//        kryo.register(SubscriptionCreated.class);
//        kryo.register(SubscriptionIteratorNext.class);
//        kryo.register(byte[].class);
//    }

//    public <T extends Message> void register(Class<T> type, Consumer<T> func) {
//        if (client.isConnected()) {
//            throw new IllegalStateException("Client already connected");
//        }
//        client.getKryo().register(type);
//        handlers.put(type, func);
//    }

    public void connect(String host, int port) {
        this.client = TcpEventClient.create()
//                .keepAlive(2, TimeUnit.SECONDS)
//                .option(Options.SEND_BUFFER, Size.MB.ofInt(100))
                .option(Options.WORKER_IO_THREADS, 1)
                .onEvent(new EventListener())
                .connect(new InetSocketAddress(host, port), 5, TimeUnit.SECONDS);
    }

    public void sendAsync(Message message) {
        client.send(message);
    }

    public <T extends Message> Response<T> send(Message message) {
        message.id = reqids.incrementAndGet();
        Response<T> response = new Response<>(message.id, responseTable::remove);
        responseTable.put(message.id, response);

        try {
            client.send(message);
        } catch (Exception e) {
            response.cancel(true);
            responseTable.remove(message.id);
        }
        return response;
    }

    private final class EventListener implements EventHandler {
        @Override
        public void onEvent(TcpConnection connection, Object data) {
            if (data instanceof Message) {
                Message msg = (Message) data;
                if (msg.id == NO_RESP) {
                    logger.warn("Received event client was not expecting");
                    return;
                }
                Response resp = responseTable.remove(msg.id);
                if (resp != null) {
                    resp.complete(msg);
//                    if(resp.timeTaken() > 500) {
//                        System.out.println("SLOW REQUEST: " + resp.timeTaken());
//                    }
                }
            } else {
                throw new IllegalStateException("Received wrong event type" + data);
            }
        }
    }

}
