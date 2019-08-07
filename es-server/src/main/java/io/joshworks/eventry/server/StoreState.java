package io.joshworks.eventry.server;

import io.joshworks.eventry.server.cluster.node.Node;
import io.joshworks.fstore.es.shared.EventId;
import io.joshworks.fstore.es.shared.Status;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class StoreState {

    private final Map<String, Node> nodes = new ConcurrentHashMap<>();
    private final Map<Long, Node> streamMapping = new ConcurrentHashMap<>();

    public void addNode(Node node, Set<Long> streams) {
        nodes.put(node.id, node);
        for (Long stream : streams) {
            streamMapping.put(stream, node);
        }
    }

    public void updateNode(String nodeId, Status status) {
        Node node = nodes.get(nodeId);
        if (node == null) {
            throw new IllegalArgumentException("Node not found for id " + nodeId);
        }
        node.status = status;
    }

    public Node getNode(String nodeId) {
        return nodes.get(nodeId);
    }

    public boolean hasNode(String nodeId) {
        return nodes.containsKey(nodeId);
    }

    public Node nodeForStream(long streamHash) {
        return streamMapping.get(streamHash);
    }

    public Node nodeForStream(String stream) {
        return streamMapping.get(EventId.hash(stream));
    }

    public List<Node> nodes() {
        return new ArrayList<>(nodes.values());
    }

}
