package io.joshworks.ilog.lsm.tree;

import io.joshworks.ilog.record.Record2;

class Node {
    Record2 key;
    Object value;

    Node left;
    Node right;
    boolean color;
    int size;

}
