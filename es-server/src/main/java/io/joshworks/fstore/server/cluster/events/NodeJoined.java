package io.joshworks.fstore.server.cluster.events;

import io.joshworks.fstore.es.shared.Node;

public class NodeJoined {

    public Node node;

    public NodeJoined(Node node) {
        this.node = node;
    }

    public NodeJoined() {
    }
}
