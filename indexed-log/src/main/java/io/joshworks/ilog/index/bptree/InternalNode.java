package io.joshworks.ilog.index.bptree;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class InternalNode<K extends Comparable<? super K>, V> extends Node<K, V> {
    private final BPlusTree<K, V> bPlusTree;
    List<Node<K, V>> children;

    InternalNode(BPlusTree<K, V> bPlusTree) {
        this.bPlusTree = bPlusTree;
        this.keys = new ArrayList<>();
        this.children = new ArrayList<>();
    }

    @Override
    V getValue(K key) {
        return getChild(key).getValue(key);
    }

    @Override
    void deleteValue(K key) {
        Node<K, V> child = getChild(key);
        child.deleteValue(key);
        if (child.isUnderflow()) {
            Node<K, V> childLeftSibling = getChildLeftSibling(key);
            Node<K, V> childRightSibling = getChildRightSibling(key);
            Node<K, V> left = childLeftSibling != null ? childLeftSibling : child;
            Node<K, V> right = childLeftSibling != null ? child
                    : childRightSibling;
            left.merge(right);
            deleteChild(right.getFirstLeafKey());
            if (left.isOverflow()) {
                Node<K, V> sibling = left.split();
                insertChild(sibling.getFirstLeafKey(), sibling);
            }
            if (bPlusTree.root.keyNumber() == 0)
                bPlusTree.root = left;
        }
    }

    @Override
    void insertValue(K key, V value) {
        Node<K, V> child = getChild(key);
        child.insertValue(key, value);
        if (child.isOverflow()) {
            Node<K, V> sibling = child.split();
            insertChild(sibling.getFirstLeafKey(), sibling);
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
        return children.get(0).getFirstLeafKey();
    }

    @Override
    List<V> getRange(K key1, BPlusTree.RangePolicy policy1, K key2,
                     BPlusTree.RangePolicy policy2) {
        return getChild(key1).getRange(key1, policy1, key2, policy2);
    }

    @Override
    void merge(Node<K, V> sibling) {
        InternalNode<K, V> node = (InternalNode<K, V>) sibling;
        keys.add(node.getFirstLeafKey());
        keys.addAll(node.keys);
        children.addAll(node.children);

    }

    @Override
    Node<K, V> split() {
        int from = keyNumber() / 2 + 1, to = keyNumber();
        InternalNode<K, V> sibling = new InternalNode<>(bPlusTree);
        sibling.keys.addAll(keys.subList(from, to));
        sibling.children.addAll(children.subList(from, to + 1));

        keys.subList(from - 1, to).clear();
        children.subList(from, to + 1).clear();

        return sibling;
    }

    @Override
    boolean isOverflow() {
        return children.size() > bPlusTree.branchingFactor;
    }

    @Override
    boolean isUnderflow() {
        return children.size() < (bPlusTree.branchingFactor + 1) / 2;
    }

    Node<K, V> getChild(K key) {
        int loc = Collections.binarySearch(keys, key);
        int childIndex = loc >= 0 ? loc + 1 : -loc - 1;
        return children.get(childIndex);
    }

    void deleteChild(K key) {
        int loc = Collections.binarySearch(keys, key);
        if (loc >= 0) {
            keys.remove(loc);
            children.remove(loc + 1);
        }
    }

    void insertChild(K key, Node<K, V> child) {
        int loc = Collections.binarySearch(keys, key);
        int childIndex = loc >= 0 ? loc + 1 : -loc - 1;
        if (loc >= 0) {
            children.set(childIndex, child);
        } else {
            keys.add(childIndex, key);
            children.add(childIndex + 1, child);
        }
    }

    Node<K, V> getChildLeftSibling(K key) {
        int loc = Collections.binarySearch(keys, key);
        int childIndex = loc >= 0 ? loc + 1 : -loc - 1;
        if (childIndex > 0)
            return children.get(childIndex - 1);

        return null;
    }

    Node<K, V> getChildRightSibling(K key) {
        int loc = Collections.binarySearch(keys, key);
        int childIndex = loc >= 0 ? loc + 1 : -loc - 1;
        if (childIndex < keyNumber())
            return children.get(childIndex + 1);

        return null;
    }
}
