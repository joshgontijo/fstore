package io.joshworks.lsm.server;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.lsmtree.LsmTree;
import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.lsm.server.messages.Delete;
import io.joshworks.lsm.server.messages.Get;
import io.joshworks.lsm.server.messages.Put;
import io.joshworks.lsm.server.messages.Replicate;
import io.joshworks.lsm.server.messages.Result;

import java.io.File;
import java.nio.ByteBuffer;

public class LocalNode<K extends Comparable<K>> implements StoreNode {

    private final LsmTree<K, byte[]> local;
    private final Serializer<K> serializer;
    private final Node node;

    public LocalNode(File rootDir, Serializer<K> serializer, Node node) {
        this.local = openStore(rootDir, serializer);
        this.serializer = serializer;
        this.node = node;
    }

    private static <K extends Comparable<K>> LsmTree<K, byte[]> openStore(File rootDir, Serializer<K> serializer) {
        return LsmTree.builder(rootDir, serializer, Serializers.VLEN_BYTE_ARRAY)
                .sstableStorageMode(StorageMode.MMAP)
                .transactionLogStorageMode(StorageMode.MMAP)
                .open();
    }

    private K deserializeKey(byte[] keyBytes) {
        //TODO try removing ByteBuffer.wrap ?
        return serializer.fromBytes(ByteBuffer.wrap(keyBytes));
    }

    @Override
    public void put(Put msg) {
        local.put(deserializeKey(msg.key), msg.value);
    }

    @Override
    public void replicate(Replicate msg) {

    }

    @Override
    public Result get(Get msg) {
        byte[] value = local.get(deserializeKey(msg.key));
        return new Result(msg.key, value);
    }

    @Override
    public void delete(Delete msg) {
        local.remove(deserializeKey(msg.key));
    }

    @Override
    public void close() {
        local.close();
    }

    @Override
    public String id() {
        return node.id;
    }
}
