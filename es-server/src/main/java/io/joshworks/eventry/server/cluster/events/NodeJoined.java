package io.joshworks.eventry.server.cluster.events;

import java.util.Set;

public class NodeJoined extends NodeInfo {

    public NodeJoined(String nodeId, String address, Set<Long> streams) {
        super(nodeId, address, streams);
    }
}
