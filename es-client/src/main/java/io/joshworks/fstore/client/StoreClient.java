package io.joshworks.fstore.client;

import io.joshworks.fstore.client.tcp.Response;
import io.joshworks.fstore.client.tcp.TcpClient;
import io.joshworks.fstore.core.hash.Hash;
import io.joshworks.fstore.core.hash.XXHash;
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
import io.joshworks.restclient.http.RestClient;
import io.joshworks.restclient.http.Unirest;

import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static io.joshworks.fstore.es.shared.HttpConstants.EVENT_TYPE_HEADER;

public class StoreClient {

    private static final String SERVERS_ENDPOINT = "/nodes";
    private static final String STREAMS_ENDPOINT = "/streams";
    private static final String METADATA = "metadata";

    private final Map<String, TcpClient> mapping = new ConcurrentHashMap<>();
    private final List<TcpClient> nodes = new ArrayList<>();

    private final Hash hasher = new XXHash();

    private final RestClient client = RestClient.builder().baseUrl("http://localhost:9000").build();

    private StoreClient(List<TcpClient> nodes, Map<String, TcpClient> streamMap) {
        this.mapping.putAll(streamMap);
        this.nodes.addAll(nodes);
    }

    public static void main(String[] args) throws MalformedURLException {

        StoreClient storeClient = StoreClient.connect(new URL("http://localhost:9000"));
        String stream = "stream-1";

        storeClient.createStream(stream);

        long start = System.currentTimeMillis();
        for (int i = 0; i < 100000000; i++) {
//            EventCreated eventCreated = storeClient.append(stream, "USER_CREATED", new UserCreated("josh", 123));
            storeClient.appendAsync(stream, "USER_CREATED", new UserCreated("josh", 123));
            if(i % 10000 == 0) {
                System.out.println(i + " IN " + (System.currentTimeMillis() - start));
                start = System.currentTimeMillis();
            }
        }

//        long start = System.currentTimeMillis();
//        for (int i = 0; i < 1000000; i++) {
//            EventCreated eventCreated = storeClient.appendHttp(stream, "USER_CREATED", new UserCreated("josh", 123));
//            if(i % 10000 == 0) {
//                System.out.println(i + " IN " + (System.currentTimeMillis() - start));
//                start = System.currentTimeMillis();
//            }
//        }


//
//        EventCreated eventCreated = storeClient.append(stream, "USER_CREATED", new UserCreated("josh", 123));
//        System.out.println("Added event: " + eventCreated);
//
//        EventRecord event = storeClient.get(stream, 0);
//        System.out.println(event.as(UserCreated.class));

    }

    public static StoreClient connect(URL bootstrapServer) {
        try (HttpResponse<Json> nodeResp = Unirest.get(bootstrapServer.toExternalForm(), SERVERS_ENDPOINT).asJson()) {
            if (!nodeResp.isSuccessful()) {
                throw new RuntimeException(nodeResp.asString());
            }
            List<NodeInfo> nodes = nodeResp.body().asListOf(NodeInfo.class);
            Map<String, TcpClient> streamMap = new HashMap<>();
            List<TcpClient> clients = new ArrayList<>();
            for (NodeInfo nodeInfo : nodes) {
                TcpClient nodeClient = new TcpClient(nodeInfo);
                nodeClient.connect(nodeInfo.host, nodeInfo.tcpPort);
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
        }
    }

    public NodeClientIterator iterator(String pattern, int fetchSize) {

        List<NodeIterator> iterators = new ArrayList<>();
        for (TcpClient node : nodes) {
            Response<SubscriptionCreated> send = node.send(new CreateSubscription(pattern));
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
        TcpClient node = mapping.get(stream);
        if (node == null) {
            throw new RuntimeException("No node for stream " + stream);
        }

        Response<EventData> response = node.send(new GetEvent(EventId.of(stream, version)));
        return response.get().record;
    }

    public EventCreated append(String stream, String type, Object event) {
        return append(stream, type, event, EventId.NO_EXPECTED_VERSION);
    }

    public EventCreated appendHttp(String stream, String type, Object event) {
        byte[] data = JsonSerializer.toBytes(event);
        HttpResponse<EventCreated> response = client.post(STREAMS_ENDPOINT, stream)
                .contentType("json")
                .header("Connection", "Keep-Alive")
                .header("Keep-Alive", "timeout=5000, max=1000000")
                .header(EVENT_TYPE_HEADER, type)
                .body(EventRecord.create(stream, type, data))
                .asObject(EventCreated.class);

        return response.body();
    }

    public EventCreated append(String stream, String type, Object event, int expectedVersion) {
        TcpClient node = getOrAssignNode(stream);

        byte[] data = JsonSerializer.toBytes(event);
        Response<EventCreated> response = node.send(new Append(expectedVersion, EventRecord.create(stream, type, data)));
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
        TcpClient node = nodeForStream(stream);

        byte[] data = JsonSerializer.toBytes(event);
        node.sendAsync(new Append(EventId.NO_EXPECTED_VERSION, EventRecord.create(stream, type, data)));
    }

    public void createStream(String name) {
        createStream(name, 0, 0, null, null); //TODO use constants
    }

    public void createStream(String name, int maxCount, int maxAge, Map<String, Integer> acl, Map<String, String> metadata) {
        if (mapping.containsKey(name)) {
            throw new IllegalArgumentException("Stream " + name + " already exist");
        }
        TcpClient node = nodeForStream(name);

        Response<Ack> response = node.send(new CreateStream(name, maxCount, maxCount, acl, metadata));
        Ack ack = response.get();
        mapping.put(name, node);
    }

    //hash is only used to create a stream, reading must use the map
    private TcpClient nodeForStream(String stream) {
        int hash = hasher.hash32(stream.getBytes(StandardCharsets.UTF_8));
        int nodeIdx = hash % nodes.size();
        return nodes.get(nodeIdx);
    }

    private TcpClient getOrAssignNode(String stream) {
        TcpClient node = mapping.get(stream);
        if (node == null) {
            node = nodeForStream(stream);
        }
        return node;
    }

}
