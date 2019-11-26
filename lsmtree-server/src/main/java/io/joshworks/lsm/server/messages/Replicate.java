package io.joshworks.lsm.server.messages;

public class Replicate {

    public final String nodeId;
    public final Object event;

    public Replicate(String nodeId, Object event) {
        this.nodeId = nodeId;
        this.event = event;
    }

    @Override
    public String   toString() {
        return "Replicate{" +
                "nodeId='" + nodeId + '\'' +
                ", event=" + event +
                '}';
    }
}
