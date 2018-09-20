package io.joshworks.eventry.server.cluster;

import java.net.InetSocketAddress;

public class Node {

    public final String id;
    public final InetSocketAddress address;

    public Node(String id, String host, int port) {
        this.id = id;
        this.address =  new InetSocketAddress(host, port);
    }

}
