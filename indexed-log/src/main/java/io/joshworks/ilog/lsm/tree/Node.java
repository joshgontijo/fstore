package io.joshworks.ilog.lsm.tree;

import io.joshworks.ilog.record.Record2;

public class Node {
    Record2 key;
    Object value;

    Node left;
    Node right;
    boolean color;
    int size;

    public Record2 record() {
        return key;
    }

}
