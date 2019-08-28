package io.joshworks.fstore.client;

import io.joshworks.eventry.network.tcp.TcpClientConnection;
import io.joshworks.eventry.network.tcp.client.TcpEventClient;
import io.joshworks.eventry.network.tcp.internal.Response;
import io.joshworks.fstore.core.hash.Hash;
import io.joshworks.fstore.core.hash.XXHash;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.es.shared.EventId;
import io.joshworks.fstore.es.shared.EventRecord;
import io.joshworks.fstore.es.shared.NodeInfo;
import io.joshworks.fstore.es.shared.StreamData;
import io.joshworks.fstore.es.shared.tcp.Ack;
import io.joshworks.fstore.es.shared.tcp.Append;
import io.joshworks.fstore.es.shared.tcp.CreateStream;
import io.joshworks.fstore.es.shared.tcp.CreateSubscription;
import io.joshworks.fstore.es.shared.tcp.EventCreated;
import io.joshworks.fstore.es.shared.tcp.EventData;
import io.joshworks.fstore.es.shared.tcp.GetEvent;
import io.joshworks.fstore.es.shared.tcp.SubscriptionCreated;
import io.joshworks.fstore.serializer.json.JsonSerializer;
import io.joshworks.restclient.http.HttpResponse;
import io.joshworks.restclient.http.Json;
import io.joshworks.restclient.http.Unirest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xnio.Options;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class StoreClient implements Closeable {

    private static final String SERVERS_ENDPOINT = "/nodes";
    private static final String STREAMS_ENDPOINT = "/streams";
    private static final String METADATA = "metadata";

    private static final Logger logger = LoggerFactory.getLogger(StoreClient.class);


    private final Partitions partitions = new Partitions();



//    private final RestClient client = RestClient.builder().baseUrl("http://localhost:9000").build();

    private StoreClient(List<Node> nodes, Map<String, Node> streamMap) {
        this.mapping.putAll(streamMap);
        this.nodes.addAll(nodes);
    }


    public static StoreClient connect(URL... bootstrapServers) {
        for (URL server : bootstrapServers) {
            try (HttpResponse<Json> nodeResp = Unirest.get(server.toExternalForm(), SERVERS_ENDPOINT).asJson()) {
                if (!nodeResp.isSuccessful()) {
                    throw new RuntimeException(nodeResp.asString());
                }
                List<NodeInfo> nodes = nodeResp.body().asListOf(NodeInfo.class);
                Map<String, Node> streamMap = new HashMap<>();
                List<Node> clients = new ArrayList<>();
                for (NodeInfo nodeInfo : nodes) {
                    TcpClientConnection client = connect(nodeInfo.host, nodeInfo.tcpPort);
                    Node nodeClient = new Node(nodeInfo, client);
                    //TODO replace with TCP
                    try (HttpResponse<Json> streamResp = Unirest.get(nodeInfo.httpAddress(), STREAMS_ENDPOINT).asJson()) {
                        if (!streamResp.isSuccessful()) {
                            throw new RuntimeException(streamResp.asString());
                        }
                        List<StreamData> serverStream = streamResp.body().asListOf(StreamData.class);
                        for (StreamData stream : serverStream) {
                            streamMap.put(stream.name, nodeClient);
                        }
                        clients.add(nodeClient);
                    }
                }
                return new StoreClient(clients, streamMap);
            } catch (Exception e) {
                logger.warn("Failed to connect to bootstrap servers", e);
            }
        }
        throw new RuntimeException("Could not connect to any of the bootstrap servers");
    }

    private static TcpClientConnection connect(String host, int port) {
        return TcpEventClient.create()
//                .keepAlive(2, TimeUnit.SECONDS)
//                .option(Options.SEND_BUFFER, Size.MB.ofInt(100))
                .option(Options.WORKER_IO_THREADS, 1)
                .bufferSize(Size.KB.ofInt(16))
                .option(Options.SEND_BUFFER, Size.KB.ofInt(16))
                .option(Options.RECEIVE_BUFFER, Size.KB.ofInt(16))
                .onEvent((connection, data) -> {
                    //TODO
                })
                .connect(new InetSocketAddress(host, port), 5, TimeUnit.SECONDS);
    }

    public NodeClientIterator iterator(String pattern, int fetchSize) {

        List<NodeIterator> iterators = new ArrayList<>();
        for (Node node : nodes) {
            Response<SubscriptionCreated> send = node.client().request(new CreateSubscription(pattern));
            String subId = send.get().subscriptionId;
            iterators.add(new NodeIterator(subId, node, fetchSize));
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
        Node node = mapping.get(stream);
        if (node == null) {
            throw new RuntimeException("No node for stream " + stream);
        }

        Response<EventData> response = node.client().request(new GetEvent(EventId.of(stream, version)));
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
        Node node = getOrAssignNode(stream);

        byte[] data = JsonSerializer.toBytes(event);
        Response<EventCreated> response = node.client().request(new Append(expectedStreamVersion, EventRecord.create(stream, type, data)));
        EventCreated created = response.get();
        if (created.version == EventId.START_VERSION) {
            mapping.put(stream, node); //success, add the node to mapped streams
        }
        return created;
    }

    public void appendAsync(String stream, String type, Object event) {
        if (!mapping.containsKey(stream)) {
            throw new IllegalArgumentException("Async append can only be executed on existing stream");
        }
        Node node = nodeForStream(stream);

        byte[] data = JsonSerializer.toBytes(event);
        node.client().send(new Append(EventId.NO_EXPECTED_VERSION, EventRecord.create(stream, type, data)));
    }

    public void createStream(String name) {
        createStream(name, 0, 0, null, null); //TODO use constants
    }

    public void createStream(String name, int maxAge, int maxCount, Map<String, Integer> acl, Map<String, String> metadata) {
        if (mapping.containsKey(name)) {
            throw new IllegalArgumentException("Stream " + name + " already exist");
        }
        Node node = nodeForStream(name);

        Response<Ack> response = node.client().request(new CreateStream(name, maxAge, maxCount, acl, metadata));
        Ack ack = response.get();
        mapping.put(name, node);
    }

    //hash is only used to create a stream, reading must use the map
    private Node nodeForStream(String stream) {
        int hash = hasher.hash32(stream.getBytes(StandardCharsets.UTF_8));
        int nodeIdx = hash % nodes.size();
        return nodes.get(nodeIdx);
    }

    private Node getOrAssignNode(String stream) {
        Node node = mapping.get(stream);
        if (node == null) {
            node = nodeForStream(stream);
        }
        return node;
    }

    @Override
    public void close() {
        for (Node node : nodes) {
            node.client().close();
        }
        nodes.clear();
    }
}
