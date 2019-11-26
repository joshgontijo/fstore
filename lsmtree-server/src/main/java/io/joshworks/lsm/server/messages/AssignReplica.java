package io.joshworks.lsm.server.messages;

public class AssignReplica {

    public String nodeId;

    public AssignReplica(String nodeId) {
        this.nodeId = nodeId;
    }

    public AssignReplica() {
    }
}
