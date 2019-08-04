package io.joshworks.fstore.client;

public class NodeStream {

    public final String nodeId;
    public final String name;
    public final long hash;

    public NodeStream(String nodeId, String name, long hash) {
        this.nodeId = nodeId;
        this.name = name;
        this.hash = hash;
    }
}
