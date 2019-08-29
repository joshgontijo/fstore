package io.joshworks.eventry.server.cluster.events;


public class NodeInfoRequest {

    public final String nodeId;

    public NodeInfoRequest(String nodeId) {
        this.nodeId = nodeId;
    }

}
