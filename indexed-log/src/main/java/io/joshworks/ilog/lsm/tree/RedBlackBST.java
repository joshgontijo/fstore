package io.joshworks.ilog.lsm.tree;

import io.joshworks.fstore.core.util.Pool;
import io.joshworks.ilog.index.IndexFunction;
import io.joshworks.ilog.index.RowKey;
import io.joshworks.ilog.record.Record2;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;

/**
 * An fixed size, {@link ByteBuffer} backed tree
 */
public class RedBlackBST implements Iterable<Node> {
    private static final boolean RED = true;
    private static final boolean BLACK = false;
    private Node root;
    private final NodePool nodePool = new NodePool();
    private final RowKey rowKey;

    public RedBlackBST(RowKey rowKey) {
        this.rowKey = rowKey;
    }

    private boolean isRed(Node x) {
        if (x == null) return false;
        return x.color == RED;
    }

    public void clear() {
        for (Node node : this) {
            nodePool.free(node);
        }
        root = null;
    }

    private int size(Node x) {
        if (x == null) return 0;
        return x.size;
    }

    public int size() {
        return size(root);
    }

    public boolean isEmpty() {
        return root == null;
    }

    public Node get(ByteBuffer key) {
        return get(root, key);
    }

    public Node apply(ByteBuffer key, IndexFunction fn) {
        if (IndexFunction.CEILING.equals(fn)) {
            return ceiling(key);
        }
        if (IndexFunction.EQUALS.equals(fn)) {
            return get(key);
        }
        if (IndexFunction.FLOOR.equals(fn)) {
            return floor(key);
        }
        //TODO ADD LOWER AND HIGHER
        throw new UnsupportedOperationException("FUNCTION NOT SUPPORTED");
    }

    private Node get(Node x, ByteBuffer key) {
        while (x != null) {
            int cmp = compareKeys(key, x);
            if (cmp < 0) x = x.left;
            else if (cmp > 0) x = x.right;
            else return x;
        }
        return null;
    }

    private int compareRecord(Record2 record, Node node) {
        return record.compare(rowKey, node.key);
    }

    private int compareKeys(ByteBuffer k1, Node node) {
        return node.key.compare(rowKey, k1);
    }

    public boolean contains(ByteBuffer key) {
        return get(key) != null;
    }

    public void put(Record2 record) {
        root = put(root, record);
        root.color = BLACK;
    }

    private Node put(Node h, Record2 record) {
        if (h == null) {
            Node node = allocateNode(record);
            node.color = RED;
            node.size = 1;
            return node;
        }

        int cmp = compareRecord(record, h);
        if (cmp < 0)
            h.left = put(h.left, record);
        else if (cmp > 0) {
            h.right = put(h.right, record);
        } else { //equals, replace
            h.key = record;
        }

        if (isRed(h.right) && !isRed(h.left))
            h = rotateLeft(h);

        return checkRotateOrFlip(h);
    }

    private Node checkRotateOrFlip(Node h) {
        if (isRed(h.left) && isRed(h.left.left)) h = rotateRight(h);
        if (isRed(h.left) && isRed(h.right)) flipColors(h);
        h.size = size(h.left) + size(h.right) + 1;
        return h;
    }

    public void deleteMin() {
        if (isEmpty()) throw new NoSuchElementException("BST underflow");

        if (!isRed(root.left) && !isRed(root.right))
            root.color = RED;
        root = deleteMin(root);
        if (!isEmpty()) root.color = BLACK;
    }

    private Node deleteMin(Node h) {
        if (h.left == null)
            return null;
        if (!isRed(h.left) && !isRed(h.left.left))
            h = moveRedLeft(h);
        h.left = deleteMin(h.left);
        return balance(h);
    }

    public void deleteMax() {
        if (isEmpty()) throw new NoSuchElementException("BST underflow");

        if (!isRed(root.left) && !isRed(root.right))
            root.color = RED;
        root = deleteMax(root);
        if (!isEmpty()) root.color = BLACK;
    }

    private Node deleteMax(Node h) {
        if (isRed(h.left))
            h = rotateRight(h);
        if (h.right == null)
            return null;
        if (!isRed(h.right) && !isRed(h.right.left))
            h = moveRedRight(h);
        h.right = deleteMax(h.right);
        return balance(h);
    }

    private Node rotateRight(Node h) {
        Node x = h.left;
        h.left = x.right;
        x.right = h;
        x.color = x.right.color;
        x.right.color = RED;
        x.size = h.size;
        h.size = size(h.left) + size(h.right) + 1;
        return x;
    }

    private Node rotateLeft(Node h) {
        Node x = h.right;
        h.right = x.left;
        x.left = h;
        x.color = x.left.color;
        x.left.color = RED;
        x.size = h.size;
        h.size = size(h.left) + size(h.right) + 1;
        return x;
    }

    private void flipColors(Node h) {

        h.color = !h.color;
        h.left.color = !h.left.color;
        h.right.color = !h.right.color;
    }

    private Node moveRedLeft(Node h) {

        flipColors(h);
        if (isRed(h.right.left)) {
            h.right = rotateRight(h.right);
            h = rotateLeft(h);
            flipColors(h);
        }
        return h;
    }

    private Node moveRedRight(Node h) {

        flipColors(h);
        if (isRed(h.left.left)) {
            h = rotateRight(h);
            flipColors(h);
        }
        return h;
    }

    private Node balance(Node h) {

        if (isRed(h.right)) h = rotateLeft(h);
        return checkRotateOrFlip(h);
    }

    public int height() {
        return height(root);
    }

    @Override
    public Iterator<Node> iterator() {
        return new BSTIterator(root);
    }

    private int height(Node x) {
        if (x == null) return -1;
        return 1 + Math.max(height(x.left), height(x.right));
    }

    public Node min() {
        if (isEmpty()) throw new NoSuchElementException("calls min() with empty symbol table");
        return min(root);
    }

    private Node min(Node x) {
        if (x.left == null) return x;
        else return min(x.left);
    }

    public Node max() {
        if (isEmpty()) throw new NoSuchElementException("calls max() with empty symbol table");
        return max(root);
    }

    private Node max(Node x) {
        if (x.right == null) return x;
        else return max(x.right);
    }

    public Node floor(ByteBuffer key) {
        if (isEmpty()) return null;
        return floor(root, key);
    }

    private Node floor(Node x, ByteBuffer key) {
        if (x == null) return null;
        int cmp = compareKeys(key, x);
        if (cmp == 0) return x;
        if (cmp < 0) return floor(x.left, key);
        Node t = floor(x.right, key);
        if (t != null) return t;
        else return x;
    }

    public Node ceiling(ByteBuffer key) {
        if (isEmpty()) return null;
        return ceiling(root, key);
    }

    private Node ceiling(Node x, ByteBuffer key) {
        if (x == null) return null;
        int cmp = compareKeys(key, x);
        if (cmp == 0) return x;
        if (cmp > 0) return ceiling(x.right, key);
        Node t = ceiling(x.left, key);
        if (t != null) return t;
        else return x;
    }

    public Record2 select(int k) {
        if (k < 0 || k >= size()) {
            throw new IllegalArgumentException("argument to select() is invalid: " + k);
        }
        Node x = select(root, k);
        return x.key;
    }

    private Node select(Node x, int k) {
        int t = size(x.left);
        if (t > k) return select(x.left, k);
        else if (t < k) return select(x.right, k - t - 1);
        else return x;
    }

    private Node allocateNode(Record2 record) {
        Node node = nodePool.allocate();
        node.value = null;
        node.key = record;
        return node;
    }

    private static class NodePool implements Pool<Node> {

        private final Queue<Node> pool = new ArrayDeque<>(1000);

        @Override
        public Node allocate() {
            Node instance = pool.poll();
            return instance == null ? new Node() : instance;
        }

        @Override
        public void free(Node node) {
            pool.offer(node);
            node.size = 0;
            clearNode(node);
        }

        private void clearNode(Node node) {
            node.value = -1;
            node.left = null;
            node.right = null;
            node.color = BLACK;
            node.size = 0;
        }

    }

}