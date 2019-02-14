package io.joshworks.eventry.server.cluster;

import io.joshworks.eventry.server.NodeStatus;
import org.jgroups.Address;

public class ClusterNode {

    public final String uuid;
    public final Address address;
    public final long since;

    public NodeStatus status = NodeStatus.UP;

    public ClusterNode(Address address) {
        this.uuid = address.toString(); //Jgroups will use the logical channel name as the address string
        this.address = address;
        this.since = System.currentTimeMillis();
    }
}
