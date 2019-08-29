package io.joshworks.eventry.server.cluster.events;

import io.joshworks.fstore.es.shared.Node;
import io.joshworks.fstore.es.shared.Status;

public class NodeInfo  {

    public Node node;

    public NodeInfo(Node node) {
        this.node = node;
    }

    public NodeInfo() {
    }
}
