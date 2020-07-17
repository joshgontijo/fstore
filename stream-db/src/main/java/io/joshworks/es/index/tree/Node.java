package io.joshworks.es.index.tree;

import java.nio.ByteBuffer;

public class Node {
    final ByteBuffer key;
    final int keyOffset;
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

    public int recordLen() {
        return len;
    }

}
