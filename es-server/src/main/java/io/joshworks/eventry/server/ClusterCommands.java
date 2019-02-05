package io.joshworks.eventry.server;

import io.joshworks.eventry.server.cluster.data.NodeInfo;

public interface ClusterCommands {

    NodeInfo onNodeInfoRequested();


}
