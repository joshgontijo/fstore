package io.joshworks.es.index.tree;

import io.joshworks.es.index.IndexEntry;
import io.joshworks.fstore.core.util.ObjectPool;

public class Node {

    public long stream;
    public int version;
    public long logAddress;

    Node left;
    Node right;
    boolean color;
    int size;


    final ObjectPool<Node> ref;

    public Node(ObjectPool<Node> ref) {
        this.ref = ref;
    }

    void init(IndexEntry ie) {
        this.stream = ie.stream();
        this.version = ie.version();
        this.logAddress = ie.logAddress();
    }

    void update(IndexEntry ie) {
        this.logAddress = ie.logAddress();
    }

}
