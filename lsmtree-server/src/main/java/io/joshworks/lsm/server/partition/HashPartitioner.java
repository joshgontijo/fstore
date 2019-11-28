package io.joshworks.lsm.server.partition;

import io.joshworks.fstore.core.hash.Hash;
import io.joshworks.fstore.core.hash.XXHash;
import io.joshworks.lsm.server.RemoteNode;
import io.joshworks.lsm.server.StoreNode;

import java.util.List;

public class HashPartitioner implements Partitioner {

    private final Hash hasher = new XXHash();

    @Override
    public StoreNode select(List<StoreNode> nodes, byte[] key) {
        int hash = hasher.hash32(key);
        return nodes.get(hash % nodes.size());
    }
}
