package io.joshworks.eventry.server.cluster.events;

import io.joshworks.eventry.network.ClusterMessage;

import java.util.Map;
import java.util.Set;

public class NodeInfo implements ClusterMessage {

    public final String nodeId;
    //partition -> [buckets]
    public final Map<String, Set<Integer>> buckets;

    public NodeInfo(String nodeId, Map<String, Set<Integer>> buckets) {
        this.nodeId = nodeId;
        this.buckets = buckets;
    }

    @Override
    public String toString() {
        return "NodeInfo{" + "nodeId='" + nodeId + '\'' +
                ", buckets=" + buckets +
                '}';
    }
}
