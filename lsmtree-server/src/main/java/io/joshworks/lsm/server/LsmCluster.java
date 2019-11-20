package io.joshworks.lsm.server;

import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.lsmtree.LsmTree;
import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.lsm.server.messages.Delete;
import io.joshworks.lsm.server.messages.Get;
import io.joshworks.lsm.server.messages.Put;
import io.joshworks.lsm.server.messages.Result;

import java.io.File;
import java.util.List;

public class LsmCluster {

    private final LsmTree<String, byte[]> lsmTree;
    private final List<Node> nodes;
    private final int replicationFactor = 3;

    public LsmCluster(File rootDir, List<Node> nodes) {
        this.nodes = nodes;
        this.lsmTree = openStore(rootDir);
    }

    private LsmTree<String, byte[]> openStore(File rootDir) {
        return LsmTree.builder(rootDir, Serializers.VSTRING, Serializers.VLEN_BYTE_ARRAY)
                .sstableStorageMode(StorageMode.MMAP)
                .transactionLogStorageMode(StorageMode.MMAP)
                .open();
    }


    public void put(Put msg) {
        lsmTree.put(msg.key, msg.value);
        if (nodes.size() < replicationFactor) {
            throw new IllegalStateException("Node not available");
        }

        for (Node node : nodes) {
            node.tcp().request(msg);
        }

    }

    public Result get(Get msg) {
        byte[] data = lsmTree.get(msg.key);
        return new Result(msg.key, data);
    }

    public void delete(Delete msg) {
        lsmTree.remove(msg.key);
    }

    public void close() {
        lsmTree.close();
    }
}
