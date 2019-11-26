package io.joshworks.lsm.server.replication;

import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.lsmtree.LsmTree;
import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.lsm.server.messages.Delete;
import io.joshworks.lsm.server.messages.Put;
import io.joshworks.lsm.server.messages.Replicate;

import java.io.File;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Replicas {

    private final Map<String, LsmTree<String, byte[]>> stores = new ConcurrentHashMap<>();

    private final File root;
    private static final String REPLICA_ROOT = "replication";

    public Replicas(File root) {
        this.root = new File(root, REPLICA_ROOT);
    }

    public void initialize(String nodeId) {
        if (stores.containsKey(nodeId)) {
            return; //TODO exception ?
        }

        LsmTree<String, byte[]> store = LsmTree.builder(new File(root, nodeId), Serializers.VSTRING, Serializers.VLEN_BYTE_ARRAY)
                .sstableStorageMode(StorageMode.MMAP)
                .transactionLogStorageMode(StorageMode.MMAP)
                .open();

        stores.put(nodeId, store);
    }

    public void replicate(Replicate event) {
        LsmTree<String, byte[]> store = stores.get(event.nodeId);
        if (store == null) {
            throw new RuntimeException("Node not found for " + event.nodeId);
        }

        if (event.event instanceof Put) {
            Put put = (Put) event.event;
            store.put(put.key, put.value);
        } else if (event.event instanceof Delete) {
            Delete delete = (Delete) event.event;
            store.remove(delete.key);
        } else {
            throw new RuntimeException("Invalid replicate payload: " + event.event);
        }

    }

}
