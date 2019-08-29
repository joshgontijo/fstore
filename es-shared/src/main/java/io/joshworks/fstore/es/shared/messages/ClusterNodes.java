package io.joshworks.fstore.es.shared.messages;

import io.joshworks.fstore.es.shared.Node;

import java.util.List;

public class ClusterNodes {
    public List<Node> nodes;

    public ClusterNodes() {
    }

    public ClusterNodes(List<Node> nodes) {
        this.nodes = nodes;
    }
}
