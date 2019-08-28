package io.joshworks.eventry.server.cluster.events;

import java.util.Set;

public class NodeJoined extends ClusterNodeInfo {

    public NodeJoined(String nodeId, String address, Set<Integer> partitions) {
        super(nodeId, address, partitions);
    }
}
