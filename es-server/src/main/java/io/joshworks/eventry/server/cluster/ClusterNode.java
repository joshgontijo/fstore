package io.joshworks.eventry.server.cluster;

import io.joshworks.eventry.server.NodeStatus;
import org.jgroups.Address;

public class ClusterNode {

    public final String uuid;
    public final Address address;
    public final long since;

    public NodeStatus status = NodeStatus.DOWN;

    public ClusterNode(String uuid, Address address, long since) {
        this.uuid = uuid;
        this.address = address;
        this.since = since;
    }
}
