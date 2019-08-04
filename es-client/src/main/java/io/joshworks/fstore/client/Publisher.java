package io.joshworks.fstore.client;

import io.joshworks.fstore.core.hash.Hash;
import io.joshworks.fstore.core.hash.XXHash;
import io.joshworks.fstore.es.shared.NodeInfo;
import io.joshworks.restclient.http.Json;
import io.joshworks.restclient.http.Response;
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

public class Publisher {


    private final RestClient client = RestClient.builder().build();

    private static final String SERVERS_ENDPOINT = "/nodes";
    private static final String STREAMS_ENDPOINT = "/streams";

    private final Map<String, String> mapping = new ConcurrentHashMap<>();
    private final List<NodeInfo> nodes = new ArrayList<>();

    private final Hash hasher = new XXHash();

    private Publisher(Map<String, String> streamMap) {
        mapping.putAll(streamMap);
    }

    public static Publisher connect(URL bootstrapServer) {
        try (Response<Json> nodeResp = Unirest.get(bootstrapServer.toExternalForm(), SERVERS_ENDPOINT).asJson()) {
            if (!nodeResp.isSuccessful()) {
                throw new RuntimeException(nodeResp.asString());
            }
            List<NodeInfo> servers = nodeResp.body().asListOf(NodeInfo.class);

            Map<String, String> streamMap = new HashMap<>();
            for (NodeInfo server : servers) {
                try (Response<Json> streamResp = Unirest.get(server.address, STREAMS_ENDPOINT).asJson()) {
                    if (!streamResp.isSuccessful()) {
                        throw new RuntimeException(streamResp.asString());
                    }
                    List<String> serverStream = streamResp.body().asListOf(String.class);
                    for (String stream : serverStream) {
                        streamMap.put(stream, server.address);
                    }
                }
            }
            return new Publisher(streamMap);
        }
    }

    public static void main(String[] args) throws MalformedURLException {

        Publisher publisher = Publisher.connect(new URL("http://localhost:9000"));
        publisher.createStream("stream-1");
        publisher.append("stream-1", "USER_CREATED", new UserCreated("josh", 123));


    }

    public void get(String stream, int version) {
        String nodeUrl = mapping.get(stream);
        if(nodeUrl == null) {
            throw new RuntimeException("No node for stream " + stream);
        }
        Response<String> response = client.get(nodeUrl)
                .contentType("json")
                .header(EVENT_TYPE_HEADER, type)
                .body(event)
                .asString();
    }

    public void append(String stream, String type, Object event) {
        String nodeUrl = mapping.get(stream);
        if(nodeUrl == null) {
            nodeUrl = nodeForStream(stream).address;
        }

        Response<String> response = client.post(nodeUrl)
                .contentType("json")
                .header(EVENT_TYPE_HEADER, type)
                .body(event)
                .asString();

        if (!response.isSuccessful()) {
            throw new RuntimeException(response.asString());
        }
    }

    public void createStream(String name) {
        if (mapping.containsKey(name)) {
            throw new IllegalArgumentException("Stream " + name + " already exist");
        }
        String url = nodeForStream(name).address;

        Response<Stream> response = client.post(url, STREAMS_ENDPOINT).body(new Stream(name)).asObject(Stream.class);
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
