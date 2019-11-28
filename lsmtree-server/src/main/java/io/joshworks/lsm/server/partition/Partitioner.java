package io.joshworks.lsm.server.partition;

import io.joshworks.lsm.server.RemoteNode;
import io.joshworks.lsm.server.StoreNode;

import java.util.List;

public interface Partitioner {

    StoreNode select(List<StoreNode> nodes, byte[] key);

}
