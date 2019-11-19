package io.joshworks.lsm.server;

import io.joshworks.fstore.lsmtree.LsmTree;
import io.joshworks.lsm.server.messages.Delete;
import io.joshworks.lsm.server.messages.Get;
import io.joshworks.lsm.server.messages.Put;
import io.joshworks.lsm.server.messages.Result;

public class LsmCluster {

    private final LsmTree<String, byte[]> lsmTree;

    public LsmCluster(LsmTree<String, byte[]> lsmTree) {
        this.lsmTree = lsmTree;
    }

    public void put(Put msg) {
        lsmTree.put(msg.key, msg.value);
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
