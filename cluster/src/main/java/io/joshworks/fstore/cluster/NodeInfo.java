package io.joshworks.fstore.cluster;

import io.joshworks.fstore.core.util.AttributeKey;
import io.joshworks.fstore.core.util.AttributeMap;
import org.jgroups.Address;

import java.net.InetSocketAddress;

public class NodeInfo {

    public final String id;
    public final Address address;
    public final long since;
    public final InetSocketAddress inetAddr;

    public final AttributeMap attachments = new AttributeMap();

    public NodeStatus status = NodeStatus.UP;

    public NodeInfo(Address address) {
        this.id = address.toString(); //Jgroups uses the logical channel name as the address string
        this.address = address;
        this.since = System.currentTimeMillis();
        this.inetAddr = null;
    }

    public NodeInfo(Address address, InetSocketAddress inetAddr) {
        this.id = address.toString(); //Jgroups uses the logical channel name as the address string
        this.address = address;
        this.since = System.currentTimeMillis();
        this.inetAddr = inetAddr;
    }

    public String hostAddress() {
        return inetAddr.getAddress().getHostAddress();
    }

    public <T> void attach(AttributeKey<T> key, T value) {
        attachments.putAttachment(key, value);
    }

    public <T> T get(AttributeKey<T> key) {
        return attachments.getAttachment(key);
    }
}
