package io.joshworks.fstore.server.cluster.events;

public class NodeLeft {

    public final String nodeId;

    public NodeLeft(String nodeId) {
        this.nodeId = nodeId;
    }


}