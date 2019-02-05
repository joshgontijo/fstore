package io.joshworks.eventry.server;

import io.joshworks.eventry.server.cluster.data.NodeInfo;
import io.joshworks.eventry.server.cluster.message.NodeJoined;
import io.joshworks.eventry.server.cluster.message.NodeLeft;

public interface ClusterEvents {

    void onNodeJoined(NodeJoined nodeJoined);

    NodeInfo onNodeInfoRequested();

    void onNodeLeft(NodeLeft nodeLeft);


}
