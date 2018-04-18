package io.joshworks.fstore.index.bplustree;

import io.joshworks.fstore.index.bplustree.storage.OffHeapBlockStore;
import io.joshworks.fstore.serializer.Serializers;

public class OffHeapBtreeTest extends BPlusTreeTestBase {
    @Override
    protected BPlusTree<Integer, String> create(int order) {
        return BPlusTree.of(new OffHeapBlockStore<>(4096, order, Serializers.INTEGER, Serializers.VSTRING));
    }
}
