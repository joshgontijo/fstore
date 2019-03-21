package io.joshworks.eventry.server.cluster;

import io.joshworks.eventry.server.NodeStatus;
import org.jgroups.Address;

public class Node {

    public final String nodeId;
    public final Address address;
    public final long since;

    public NodeStatus status = NodeStatus.UP;

    public Node(Address address) {
        this.nodeId = address.toString(); //Jgroups will use the logical channel name as the address string
        this.address = address;
        this.since = System.currentTimeMillis();
    }
}
