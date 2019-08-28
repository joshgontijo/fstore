package io.joshworks.eventry.server.cluster.events;

import java.util.Set;

public class ClusterNodeInfo {

    public final String nodeId;
    public final String address;
    public final Set<Integer> partitions;

    public ClusterNodeInfo(String nodeId, String address, Set<Integer> partitions) {
        this.nodeId = nodeId;
        this.address = address;
        this.partitions = partitions;
    }

    @Override
    public String toString() {
        return "NodeInfo{" + "nodeId='" + nodeId + '\'' +
                ", address='" + address + '\'' +
                ", streamsSize=" + partitions.size() +
                '}';
    }
}
