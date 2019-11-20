package io.joshworks.fstore.server.cluster.events;

import io.joshworks.fstore.es.shared.Node;

public class NodeInfo  {

    public Node node;

    public NodeInfo(Node node) {
        this.node = node;
    }

    public NodeInfo() {
    }
}
