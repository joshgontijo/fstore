package io.joshworks.eventry.server.cluster.data;

import java.net.InetSocketAddress;

public class NodeLeft  {

    public final InetSocketAddress address;

    public NodeLeft(InetSocketAddress address) {
        this.address = address;
    }
}
