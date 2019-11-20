package io.joshworks.fstore.server;

import io.joshworks.fstore.es.shared.Node;
import io.joshworks.fstore.es.shared.Status;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ClusterState {

    //TODO this is not consistent, it should be sorted based on some field in odrder to get the hashing algorithm to work properly
    private final Map<String, Node> nodes = new ConcurrentHashMap<>();
    private final String thisNode;

    public ClusterState(String thisNode) {
        this.thisNode = thisNode;
    }

    public void update(Node node) {
        nodes.put(node.id, node);
    }

    public void update(String nodeId, Status status) {
        Node node = nodes.get(nodeId);
        if(node != null) {
            node.status = status;
        }
    }

    public Node getNode(String nodeId) {
        return nodes.get(nodeId);
    }

    public Node thisNode() {
        return getNode(thisNode);
    }

    public List<Node> all() {
        return new ArrayList<>(nodes.values());
    }
}
