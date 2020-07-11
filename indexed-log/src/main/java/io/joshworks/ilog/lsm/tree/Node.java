package io.joshworks.ilog.lsm.tree;

import io.joshworks.ilog.record.Record;

public class Node {
    Record key;
    Object value;

    Node left;
    Node right;
    boolean color;
    int size;

    public Record record() {
        return key;
    }

}
