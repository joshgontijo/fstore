package io.joshworks.eventry.server.cluster.events;

import java.util.Set;

public class NodeInfo  {

    public final String nodeId;
    public final String address;
    public final Set<Long> streams;

    public NodeInfo(String nodeId, String address, Set<Long> streams) {
        this.nodeId = nodeId;
        this.address = address;
        this.streams = streams;
    }

    @Override
    public String toString() {
        return "NodeInfo{" + "nodeId='" + nodeId + '\'' +
                ", address='" + address + '\'' +
                ", streamsSize=" + streams.size() +
                '}';
    }
}
