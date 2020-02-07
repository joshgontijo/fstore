package io.joshworks.ilog.index.bptree;

import java.util.List;

abstract class Node<K extends Comparable<? super K>, V> {
    List<K> keys;

    int keyNumber() {
        return keys.size();
    }

    abstract V getValue(K key);

    abstract void deleteValue(K key);

    abstract void insertValue(K key, V value);

    abstract K getFirstLeafKey();

    abstract List<V> getRange(K key1, BPlusTree.RangePolicy policy1, K key2,
                              BPlusTree.RangePolicy policy2);

    abstract void merge(Node<K, V> sibling);

    abstract Node<K, V> split();

    abstract boolean isOverflow();

    abstract boolean isUnderflow();

    public String toString() {
        return keys.toString();
    }
}
