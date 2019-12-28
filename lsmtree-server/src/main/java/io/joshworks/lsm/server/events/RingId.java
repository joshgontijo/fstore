package io.joshworks.lsm.server.events;

public class RingId {

    public final String nodeId;
    public final int ringId;

    public RingId(String nodeId, int ringId) {
        this.nodeId = nodeId;
        this.ringId = ringId;
    }
}
