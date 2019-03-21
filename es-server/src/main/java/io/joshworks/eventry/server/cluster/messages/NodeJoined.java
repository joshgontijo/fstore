package io.joshworks.eventry.server.cluster.messages;

import io.joshworks.eventry.network.ClusterMessage;
import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.fstore.serializer.collection.SetSerializer;

import java.util.Set;

public class NodeJoined implements ClusterMessage {

    public final String nodeId;
    public final Set<Integer> partitions;

    public NodeJoined(String nodeId, Set<Integer> partitions) {
        this.nodeId = nodeId;
        this.partitions = partitions;
    }

    @Override
    public String toString() {
        return "NodeJoined{" + "nodeId='" + nodeId + '\'' +
                ", partitions=" + partitions +
                '}';
    }
}
