package io.joshworks.eventry.network;

import io.joshworks.eventry.network.util.AttachmentMap;
import org.jgroups.Address;

import java.net.InetAddress;
import java.net.InetSocketAddress;

public class ClusterNode {

    public final String uuid;
    public final Address address;
    public final long since;
    public final InetSocketAddress inetAddr;

    public final AttachmentMap attachments = new AttachmentMap();

    public NodeStatus status = NodeStatus.UP;

    public ClusterNode(Address address) {
        this.uuid = address.toString(); //Jgroups uses the logical channel name as the address string
        this.address = address;
        this.since = System.currentTimeMillis();
        this.inetAddr = null;
    }

    public ClusterNode(Address address, InetSocketAddress inetAddr) {
        this.uuid = address.toString(); //Jgroups uses the logical channel name as the address string
        this.address = address;
        this.since = System.currentTimeMillis();
        this.inetAddr = inetAddr;
    }
}
