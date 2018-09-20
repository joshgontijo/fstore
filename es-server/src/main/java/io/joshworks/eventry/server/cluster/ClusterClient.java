package io.joshworks.eventry.server.cluster;

import io.joshworks.stream.client.StreamClient;
import io.joshworks.stream.client.ws.WsConnection;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ClusterClient {

    private final Map<String, WsConnection> clients = new ConcurrentHashMap<>();

    public void createClient(String nodeId, InetSocketAddress address) {

        WsConnection wsClient = StreamClient.ws("ws://" + address.getHostString())
                .onConnect(channel -> System.out.println("WS Connected"))
                .onBinary((channel, data) -> System.out.println("Data received"))
                .onError((channel, ex) -> ex.printStackTrace())
                .connect();

        clients.put(nodeId, wsClient);
    }

    public void removeClient(String nodeId) {
        WsConnection removed = clients.remove(nodeId);
        if(removed != null) {
            removed.close();
        }
    }

    public void send(String nodeId, ByteBuffer data) {
        WsConnection conn = clients.get(nodeId);
        if(conn == null) {
            throw new RuntimeException("No node for id " + nodeId);
        }
        conn.sendBinary(data);
    }

}
