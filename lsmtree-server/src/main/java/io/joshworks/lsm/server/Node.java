package io.joshworks.lsm.server;

import java.net.InetSocketAddress;

public class Node {

    public final String id;
    public final int ringId;
    public final String host;
    public final int replicationPort;
    public final int tcpPort;
    public Status status;

    public Node(String id, int ringId, String host, int replicationPort, int tcpPort, Status status) {
        this.id = id;
        this.ringId = ringId;
        this.host = host;
        this.replicationPort = replicationPort;
        this.tcpPort = tcpPort;
        this.status = status;
    }

    public InetSocketAddress replicationTcp() {
        return new InetSocketAddress(host, replicationPort);
    }

    public InetSocketAddress tcp() {
        return new InetSocketAddress(host, tcpPort);
    }

    @Override
    public String toString() {
        return "Node{" +
                "id='" + id + '\'' +
                ", ringId=" + ringId +
                ", host='" + host + '\'' +
                ", replicationPort=" + replicationPort +
                ", tcpPort=" + tcpPort +
                ", status=" + status +
                '}';
    }
}
