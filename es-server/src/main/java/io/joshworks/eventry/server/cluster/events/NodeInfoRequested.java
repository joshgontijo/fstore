package io.joshworks.eventry.server.cluster.events;


public class NodeInfoRequested  {

    public final String nodeId;

    public NodeInfoRequested(String nodeId) {
        this.nodeId = nodeId;
    }

}
