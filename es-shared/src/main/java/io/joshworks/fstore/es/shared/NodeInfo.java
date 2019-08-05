package io.joshworks.fstore.es.shared;


import io.joshworks.eventry.network.ClusterMessage;
import io.joshworks.eventry.network.NodeStatus;

public class NodeInfo implements ClusterMessage {

    public final String id;
    public final String address;
    public final NodeStatus status;


    public NodeInfo(String id, String address, int port, NodeStatus status) {
        this.id = id;
        this.address = "http://" + address + ":" + port;
        this.status = status;
    }

    @Override
    public String toString() {
        return "NodeInfo{" + "id='" + id + '\'' +
                ", address='" + address + '\'' +
                ", status=" + status +
                '}';
    }
}
