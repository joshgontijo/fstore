package io.joshworks.ilog.index.bptree;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

class LeafNode<K extends Comparable<? super K>, V> extends Node<K, V> {
    private final BPlusTree<K, V> bPlusTree;
    List<V> values;
    LeafNode<K, V> next;

    LeafNode(BPlusTree<K, V> bPlusTree) {
        this.bPlusTree = bPlusTree;
        keys = new ArrayList<>();
        values = new ArrayList<>();
    }

    @Override
    V getValue(K key) {
        int loc = Collections.binarySearch(keys, key);
        return loc >= 0 ? values.get(loc) : null;
    }

    @Override
    void deleteValue(K key) {
        int loc = Collections.binarySearch(keys, key);
        if (loc >= 0) {
            keys.remove(loc);
            values.remove(loc);
        }
    }

    @Override
    void insertValue(K key, V value) {
        int loc = Collections.binarySearch(keys, key);
        int valueIndex = loc >= 0 ? loc : -loc - 1;
        if (loc >= 0) {
            values.set(valueIndex, value);
        } else {
            keys.add(valueIndex, key);
            values.add(valueIndex, value);
        }
        if (bPlusTree.root.isOverflow()) {
            Node<K, V> sibling = split();
            InternalNode<K, V> newRoot = new InternalNode<>(bPlusTree);
            newRoot.keys.add(sibling.getFirstLeafKey());
            newRoot.children.add(this);
            newRoot.children.add(sibling);
            bPlusTree.root = newRoot;
        }
    }

    @Override
    K getFirstLeafKey() {
        return keys.get(0);
    }

    @Override
    List<V> getRange(K key1, BPlusTree.RangePolicy policy1, K key2,
                     BPlusTree.RangePolicy policy2) {
        List<V> result = new LinkedList<>();
        LeafNode<K, V> node = this;
        while (node != null) {
            Iterator<K> kIt = node.keys.iterator();
            Iterator<V> vIt = node.values.iterator();
            while (kIt.hasNext()) {
                K key = kIt.next();
                V value = vIt.next();
                int cmp1 = key.compareTo(key1);
                int cmp2 = key.compareTo(key2);
                if (((policy1 == BPlusTree.RangePolicy.EXCLUSIVE && cmp1 > 0) || (policy1 == BPlusTree.RangePolicy.INCLUSIVE && cmp1 >= 0))
                        && ((policy2 == BPlusTree.RangePolicy.EXCLUSIVE && cmp2 < 0) || (policy2 == BPlusTree.RangePolicy.INCLUSIVE && cmp2 <= 0)))
                    result.add(value);
                else if ((policy2 == BPlusTree.RangePolicy.EXCLUSIVE && cmp2 >= 0)
                        || (policy2 == BPlusTree.RangePolicy.INCLUSIVE && cmp2 > 0))
                    return result;
            }
            node = node.next;
        }
        return result;
    }

    @Override
    void merge(Node<K, V> sibling) {
        LeafNode<K, V> node = (LeafNode<K, V>) sibling;
        keys.addAll(node.keys);
        values.addAll(node.values);
        next = node.next;
    }

    @Override
    Node<K, V> split() {
        LeafNode<K, V> sibling = new LeafNode<>(bPlusTree);
        int from = (keyNumber() + 1) / 2, to = keyNumber();
        sibling.keys.addAll(keys.subList(from, to));
        sibling.values.addAll(values.subList(from, to));

        keys.subList(from, to).clear();
        values.subList(from, to).clear();

        sibling.next = next;
        next = sibling;
        return sibling;
    }

    @Override
    boolean isOverflow() {
        return values.size() > bPlusTree.branchingFactor - 1;
    }

    @Override
    boolean isUnderflow() {
        return values.size() < bPlusTree.branchingFactor / 2;
    }
}
