package io.joshworks.eventry.server.cluster.data;

import java.net.InetSocketAddress;

public class NodeJoined  {

    public final InetSocketAddress address;

    public NodeJoined(InetSocketAddress address) {
        this.address = address;
    }
}
