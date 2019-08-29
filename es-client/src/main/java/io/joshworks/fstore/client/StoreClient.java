package io.joshworks.fstore.client;

import io.joshworks.eventry.network.tcp.TcpClientConnection;
import io.joshworks.eventry.network.tcp.client.TcpEventClient;
import io.joshworks.eventry.network.tcp.internal.Response;
import io.joshworks.eventry.network.tcp.internal.ResponseTable;
import io.joshworks.fstore.es.shared.routing.Router;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.es.shared.EventId;
import io.joshworks.fstore.es.shared.EventRecord;
import io.joshworks.fstore.es.shared.Node;
import io.joshworks.fstore.es.shared.messages.Ack;
import io.joshworks.fstore.es.shared.messages.Append;
import io.joshworks.fstore.es.shared.messages.ClusterInfoRequest;
import io.joshworks.fstore.es.shared.messages.ClusterNodes;
import io.joshworks.fstore.es.shared.messages.CreateStream;
import io.joshworks.fstore.es.shared.messages.CreateSubscription;
import io.joshworks.fstore.es.shared.messages.EventCreated;
import io.joshworks.fstore.es.shared.messages.EventData;
import io.joshworks.fstore.es.shared.messages.GetEvent;
import io.joshworks.fstore.es.shared.messages.SubscriptionCreated;
import io.joshworks.fstore.serializer.json.JsonSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xnio.Options;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class StoreClient implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(StoreClient.class);

    private final Map<Node, TcpClientConnection> connections = new ConcurrentHashMap<>();
    private final List<Node> nodes = new ArrayList<>();

    //shared response table, allowing tco clients to submit a request and wait for a response from any server
    private final ResponseTable responseTable = new ResponseTable();
    private final Router router;

    private StoreClient(Router router) {
        this.router = router;
    }

    public static StoreClient connect(Router router, InetSocketAddress... bootstrapServers) {
        StoreClient storeClient = new StoreClient(router);
        for (InetSocketAddress bootstrapServer : bootstrapServers) {
            TcpClientConnection client = connect(bootstrapServer, storeClient.responseTable);
            try {
                Response<ClusterNodes> response = client.request(new ClusterInfoRequest());
                ClusterNodes receivedNodes = response.get();
                for (Node node : receivedNodes.nodes) {
                    storeClient.nodes.add(node);
                    storeClient.connections.put(node, connect(node.tcp(), storeClient.responseTable));
                }
                return storeClient;
            } catch (Exception e) {
                logger.warn("Failed to connect to bootstrap servers", e);
            } finally {
                IOUtils.closeQuietly(client);
            }
        }
        IOUtils.closeQuietly(storeClient);
        throw new RuntimeException("Could not connect to any of the bootstrap servers");
    }

    private static TcpClientConnection connect(InetSocketAddress address, ResponseTable responseTable) {
        return TcpEventClient.create()
//                .keepAlive(2, TimeUnit.SECONDS)
//                .option(Options.SEND_BUFFER, Size.MB.ofInt(100))
                .option(Options.WORKER_IO_THREADS, 1)
                .bufferSize(Size.KB.ofInt(16))
                .option(Options.SEND_BUFFER, Size.KB.ofInt(16))
                .option(Options.RECEIVE_BUFFER, Size.KB.ofInt(16))
                .responseTable(responseTable)
                .onEvent((connection, data) -> {
                    //TODO
                })
                .connect(address, 5, TimeUnit.SECONDS);
    }

    public NodeClientIterator iterator(String pattern, int fetchSize) {

        List<NodeIterator> iterators = new ArrayList<>();
        for (TcpClientConnection conn : connections.values()) {
            Response<SubscriptionCreated> send = conn.request(new CreateSubscription(pattern));
            String subId = send.get().subscriptionId;
            iterators.add(new NodeIterator(subId, conn, fetchSize));
        }
        return new NodeClientIterator(iterators);
    }

    public Subscription subscribe(String pattern) {
        throw new UnsupportedOperationException("TODO");
//        Set<String> matches = mapping.keySet()
//                .stream()
//                .filter(stream -> StreamPattern.matches(stream, pattern))
//                .collect(Collectors.toSet());
    }

    public EventRecord get(String stream, int version) {
        Response<EventData> response = select(stream).request(new GetEvent(EventId.of(stream, version)));
        return response.get().record;
    }

    public EventCreated append(String stream, String type, Object event) {
        return append(stream, type, event, EventId.NO_EXPECTED_VERSION);
    }

//    public EventCreated appendHttp(String stream, String type, Object event) {
//        byte[] data = JsonSerializer.toBytes(event);
//        HttpResponse<EventCreated> response = client.post(STREAMS_ENDPOINT, stream)
//                .contentType("json")
//                .header("Connection", "Keep-Alive")
//                .header("Keep-Alive", "timeout=5000, max=1000000")
//                .header(EVENT_TYPE_HEADER, type)
//                .body(EventRecord.create(stream, type, data))
//                .asObject(EventCreated.class);
//
//        return response.body();
//    }

    public EventCreated append(String stream, String type, Object event, int expectedStreamVersion) {
        byte[] data = JsonSerializer.toBytes(event);
        Response<EventCreated> response = select(stream).request(new Append(expectedStreamVersion, EventRecord.create(stream, type, data)));
        return response.get();
    }

    public void appendAsync(String stream, String type, Object event) {
        byte[] data = JsonSerializer.toBytes(event);
        select(stream).send(new Append(EventId.NO_EXPECTED_VERSION, EventRecord.create(stream, type, data)));
    }

    public void createStream(String name) {
        createStream(name, 0, 0, null, null); //TODO use constants
    }

    public void createStream(String name, int maxAge, int maxCount, Map<String, Integer> acl, Map<String, String> metadata) {
        Response<Ack> response = select(name).request(new CreateStream(name, maxAge, maxCount, acl, metadata));
        Ack ack = response.get();
    }

    private TcpClientConnection select(String stream) {
        Node node = router.route(nodes, stream);
        if (node == null) {
            throw new IllegalStateException("Node must not be null for stream: " + stream);
        }
        return connections.get(node);
    }

    @Override
    public void close() {
        connections.values().forEach(IOUtils::closeQuietly);
        connections.clear();
    }
}
