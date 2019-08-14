package io.joshworks.fstore.es.shared;

public class NodeInfo {

    public final String id;
    public final String host;
    public final int httpPort;
    public final int tcpPort;
    public final Status status;

    public NodeInfo(String id, String host, int httpPort, int tcpPort, Status status) {
        this.id = id;
        this.host = host;
        this.httpPort = httpPort;
        this.tcpPort = tcpPort;
        this.status = status;
    }

    public String httpAddress() {
        return "http://" + host + ":" + httpPort;
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
