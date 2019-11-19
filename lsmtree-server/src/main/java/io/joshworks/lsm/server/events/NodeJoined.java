package io.joshworks.lsm.server.events;


import io.joshworks.lsm.server.Node;

public class NodeJoined {

    public Node node;

    public NodeJoined(Node node) {
        this.node = node;
    }

    public NodeJoined() {
    }
}
