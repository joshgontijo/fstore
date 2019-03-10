package io.joshworks.eventry.server.cluster.messages;

import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.fstore.serializer.collection.SetSerializer;

import java.util.Set;

public class NodeJoined implements ClusterMessage {

    private static final SetSerializer<Integer> setSerializer = new SetSerializer<>(Serializers.INTEGER, a -> Integer.BYTES);

    public final String nodeId;
    public final Set<Integer> partitions;


    public NodeJoined(String nodeId, Set<Integer> partitions) {
        this.nodeId = nodeId;
        this.partitions = partitions;
    }
}
