package io.joshworks.fstore.index.bplustree;


import io.joshworks.fstore.index.Entry;
import io.joshworks.fstore.index.Range;
import io.joshworks.fstore.index.bplustree.storage.BlockStore;
import io.joshworks.fstore.index.bplustree.util.DeleteResult;
import io.joshworks.fstore.index.bplustree.util.InsertResult;
import io.joshworks.fstore.index.bplustree.util.Result;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class BPlusTree<K extends Comparable<K>, V> implements Tree<K, V> {

    /**
     * The branching factor used when none specified in constructor.
     */
    private static final int DEFAULT_BRANCHING_FACTOR = 128;

    /**
     * The branching factor for the B+ tree, that measures the capacity of nodes
     * (i.e., the number of children nodes) for internal nodes in the tree.
     */
    final int order;
    private int rootId;
    private final BlockStore<K, V> store;
    private int size;
    private int height;

    private BPlusTree(BlockStore<K, V> store, int order) {
        if (order <= 2) {
            throw new IllegalArgumentException("B+Tree order must be greater than 2");
        }
        this.order = order;
        this.store = store;
        this.rootId = store.placeBlock(new LeafNode<>(order, store));
    }

    public static <K extends Comparable<K>, V> BPlusTree<K, V> of(BlockStore<K, V> store) {
        return of(store, DEFAULT_BRANCHING_FACTOR);
    }

    public static <K extends Comparable<K>, V> BPlusTree<K, V> of(BlockStore<K, V> store, int order) {
        return new BPlusTree<>(store, order);
    }

    /**
     * Adds a new entry to the index, replacing if the key already exist
     *
     * @param key   The key to be inserted
     * @param value The value to be inserted
     * @return The previous value associated with this key
     * @throws IllegalArgumentException If the key is null
     */
    @Override
    public V put(K key, V value) {
        if (key == null) {
            throw new IllegalArgumentException("Key must be provided");
        }
        Node<K, V> root = store.readBlock(rootId);
        InsertResult<V> insertResult = root.insertValue(key, value, root);
        if (insertResult.newRootId != Result.NO_NEW_ROOT) {
            rootId = insertResult.newRootId;
            height++;
        }
        if (insertResult.foundValue() == null)
            size++;
        return insertResult.foundValue();
    }

    @Override
    public V get(K key) {
        if (key == null) {
            throw new IllegalArgumentException("Key must be provided");
        }
        Node<K, V> root = store.readBlock(rootId);
        return root.getValue(key);
    }

    @Override
    public void clear() {
        store.clear();
        rootId = store.placeBlock(new LeafNode<>(order, store));
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public Iterator<Entry<K, V>> iterator() {
        return TreeIterator.iterator(store, rootId);
    }

    public Iterator<Entry<K, V>> iterator(Range<K> range) {
        return TreeIterator.iterator(store, rootId, range);
    }


    @Override
    public int height() {
        return height;
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public V remove(K key) {
        Node<K, V> root = store.readBlock(rootId);
        DeleteResult<V> deleteResult = root.deleteValue(key, root);
        if (deleteResult.newRootId != Result.NO_NEW_ROOT) {
            rootId = deleteResult.newRootId;
            height--;
        }
        if (deleteResult.foundValue() != null)
            size--;
        return deleteResult.foundValue();
    }


    @Override
    public String toString() {
        Node<K, V> root = store.readBlock(rootId);

        Queue<List<Node<K, V>>> queue = new LinkedList<>();
        queue.add(Collections.singletonList(root));
        StringBuilder sb = new StringBuilder();
        while (!queue.isEmpty()) {
            Queue<List<Node<K, V>>> nextQueue = new LinkedList<>();
            while (!queue.isEmpty()) {
                List<Node<K, V>> nodes = queue.remove();
                sb.append('{');
                Iterator<Node<K, V>> it = nodes.iterator();
                while (it.hasNext()) {
                    Node<K, V> node = it.next();
                    sb.append(node.toString());
                    if (it.hasNext())
                        sb.append(", ");
                    if (node.type == Node.INTERNAL_NODE) {
                        List<Integer> children = ((InternalNode<K, V>) node).children;

                        List<Node<K, V>> childrenNodes = new ArrayList<>();
                        for (Integer childId : children) {
                            if (childId >= 0)
                                childrenNodes.add(store.readBlock(childId));
                        }
                        nextQueue.add(childrenNodes);
                    }

                }
                sb.append('}');
                if (!queue.isEmpty())
                    sb.append(", ");
                else
                    sb.append('\n');
            }
            queue = nextQueue;
        }

        return sb.toString();
    }

}
