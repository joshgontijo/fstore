package io.joshworks.lsm.server;

import java.net.InetSocketAddress;

public class NodeInfo {

    public final String id;
    public final int ringId;
    public final String host;
    public final int httpPort;
    public final int tcpPort;
    public Status status;

    public NodeInfo(String id, int ringId, String host, int httpPort, int tcpPort, Status status) {
        this.id = id;
        this.ringId = ringId;
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
