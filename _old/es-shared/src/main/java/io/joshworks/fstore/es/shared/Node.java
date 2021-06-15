package io.joshworks.fstore.es.shared;

import java.net.InetSocketAddress;

public class Node {

    public final String id;
    public final String host;
    public final int httpPort;
    public final int tcpPort;
    public Status status;

    public Node(String id, String host, int httpPort, int tcpPort, Status status) {
        this.id = id;
        this.host = host;
        this.httpPort = httpPort;
        this.tcpPort = tcpPort;
        this.status = status;
    }

    public InetSocketAddress http() {
        return new InetSocketAddress(host, httpPort);
    }

    public InetSocketAddress tcp() {
        return new InetSocketAddress(host, tcpPort);
    }

    @Override
    public String toString() {
        return "NodeInfo{" + "id='" + id + '\'' +
                ", host='" + host + '\'' +
                ", port=" + httpPort +
                ", status=" + status +
                '}';
    }
}
