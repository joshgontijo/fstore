package io.joshworks.eventry.server.cluster.data;

public class Node {

    public final int port;
    public final String host;

    public Node(int port, String host) {
        this.port = port;
        this.host = host;
    }
}
