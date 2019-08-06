package io.joshworks.fstore.client;

import io.joshworks.fstore.core.hash.Hash;
import io.joshworks.fstore.core.hash.XXHash;
import io.joshworks.fstore.es.shared.EventHeader;
import io.joshworks.fstore.es.shared.JsonEvent;
import io.joshworks.fstore.es.shared.NodeInfo;
import io.joshworks.fstore.es.shared.StreamData;
import io.joshworks.fstore.es.shared.streams.StreamPattern;
import io.joshworks.restclient.http.HttpResponse;
import io.joshworks.restclient.http.Json;
import io.joshworks.restclient.http.MediaType;
import io.joshworks.restclient.http.RestClient;
import io.joshworks.restclient.http.Unirest;

import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static io.joshworks.fstore.es.shared.HttpConstants.EVENT_TYPE_HEADER;

public class StoreClient {

    private final RestClient client = RestClient.builder().build();

    private static final String SERVERS_ENDPOINT = "/nodes";
    private static final String STREAMS_ENDPOINT = "/streams";
    private static final String METADATA = "metadata";

    private final Map<String, String> mapping = new ConcurrentHashMap<>();
    private final List<NodeInfo> nodes = new ArrayList<>();

    private final Hash hasher = new XXHash();

    private StoreClient(List<NodeInfo> nodes, Map<String, String> streamMap) {
        this.mapping.putAll(streamMap);
        this.nodes.addAll(nodes);
    }

    public static void main(String[] args) throws MalformedURLException {

        StoreClient storeClient = StoreClient.connect(new URL("http://localhost:9000"));
        storeClient.createStream("stream-1");
        EventHeader eventHeader = storeClient.append("stream-1", "USER_CREATED", new UserCreated("josh", 123));
        System.out.println("Added event: " + eventHeader);

        JsonEvent event = storeClient.get("stream-1", 0);
        System.out.println(event.as(UserCreated.class));

    }

    public static StoreClient connect(URL bootstrapServer) {
        try (HttpResponse<Json> nodeResp = Unirest.get(bootstrapServer.toExternalForm(), SERVERS_ENDPOINT).asJson()) {
            if (!nodeResp.isSuccessful()) {
                throw new RuntimeException(nodeResp.asString());
            }
            List<NodeInfo> nodes = nodeResp.body().asListOf(NodeInfo.class);

            Map<String, String> streamMap = new HashMap<>();
            for (NodeInfo node : nodes) {
                try (HttpResponse<Json> streamResp = Unirest.get(node.address, STREAMS_ENDPOINT).asJson()) {
                    if (!streamResp.isSuccessful()) {
                        throw new RuntimeException(streamResp.asString());
                    }
                    List<StreamData> serverStream = streamResp.body().asListOf(StreamData.class);
                    for (StreamData stream : serverStream) {
                        streamMap.put(stream.name, node.address);
                    }
                }
            }
            return new StoreClient(nodes, streamMap);
        }
    }

    public Subscription subscribe(String pattern, Consumer<JsonEvent> handler) {
        Set<String> matches = mapping.keySet()
                .stream()
                .filter(stream -> StreamPattern.matches(stream, pattern))
                .collect(Collectors.toSet());
    }

    public JsonEvent get(String stream, int version) {
        String nodeUrl = mapping.get(stream);
        if (nodeUrl == null) {
            throw new RuntimeException("No node for stream " + stream);
        }
        HttpResponse<JsonEvent> response = client.get(nodeUrl, STREAMS_ENDPOINT, stream + "@" + version).asObject(JsonEvent.class);
        if(!response.isSuccessful()) {
            throw new RuntimeException(response.getStatus() + " -> " + response.asString());
        }
        return response.body();
    }

    public EventHeader append(String stream, String type, Object event) {
        String nodeUrl = mapping.get(stream);
        if (nodeUrl == null) {
            nodeUrl = nodeForStream(stream).address;
        }

        HttpResponse<Json> response = client.post(nodeUrl, STREAMS_ENDPOINT, stream)
                .contentType("json")
                .header(EVENT_TYPE_HEADER, type)
                .body(event)
                .asJson();

        if (!response.isSuccessful()) {
            throw new RuntimeException(response.getStatus() + " -> " + response.asString());
        }

        EventHeader eventHeader = response.body().as(EventHeader.class);

        if(!mapping.containsKey(eventHeader.stream)) {
            mapping.put(eventHeader.stream, nodeUrl);
        }

        return response.body().as(EventHeader.class);
    }

    public void createStream(String name) {
        if (mapping.containsKey(name)) {
            throw new IllegalArgumentException("Stream " + name + " already exist");
        }
        String url = nodeForStream(name).address;

        HttpResponse<Stream> response = client.post(url, STREAMS_ENDPOINT)
                .contentType(MediaType.APPLICATION_JSON_TYPE)
                .body(new Stream(name))
                .asObject(Stream.class);

        if (!response.isSuccessful()) {
            throw new RuntimeException("Failed to create stream");
        }

    }

    //hash is only used to create a stream, reading must use the map
    private NodeInfo nodeForStream(String stream) {
        int hash = hasher.hash32(stream.getBytes(StandardCharsets.UTF_8));
        int nodeIdx = hash % nodes.size();
        return nodes.get(nodeIdx);
    }

}
