package io.joshworks.fstore.client;

import io.joshworks.eventry.network.tcp.TcpClientConnection;
import io.joshworks.fstore.es.shared.NodeInfo;

public class Node {

    private final NodeInfo nodeInfo;
    private final TcpClientConnection client;

    public Node(NodeInfo nodeInfo, TcpClientConnection client) {
        this.nodeInfo = nodeInfo;
        this.client = client;
    }

    public NodeInfo nodeInfo() {
        return nodeInfo;
    }

    public TcpClientConnection client() {
        return client;
    }
}
