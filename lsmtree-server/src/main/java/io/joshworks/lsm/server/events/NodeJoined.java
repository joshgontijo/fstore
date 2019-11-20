package io.joshworks.lsm.server.events;


import io.joshworks.lsm.server.NodeInfo;

public class NodeJoined {

    public NodeInfo nodeInfo;

    public NodeJoined(NodeInfo nodeInfo) {
        this.nodeInfo = nodeInfo;
    }

    public NodeJoined() {
    }
}
