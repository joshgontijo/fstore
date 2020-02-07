package io.joshworks.ilog.lsm.tree;

import java.nio.ByteBuffer;

public class Node {
    public final ByteBuffer key;
    public final int keyOffset;
    int value;
    int len;

    Node left;
    Node right;
    boolean color;
    int size;

    Node(ByteBuffer dataRef, int keyOffset) {
        this.key = dataRef;
        this.keyOffset = keyOffset;
    }

    public int offset() {
        return value;
    }

    public int len() {
        return len;
    }

    public int keyLen() {
        return key.remaining();
    }

}
