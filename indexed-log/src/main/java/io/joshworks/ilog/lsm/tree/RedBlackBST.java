package io.joshworks.ilog.lsm.tree;

import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.Pool;
import io.joshworks.ilog.Record2;
import io.joshworks.ilog.index.KeyComparator;

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
    private final KeyComparator comparator;
    private final int maxEntries;
    private final NodePool nodePool;

    public RedBlackBST(KeyComparator comparator, int maxEntries, boolean direct) {
        this.comparator = comparator;
        this.maxEntries = maxEntries;
        this.nodePool = new NodePool(comparator.keySize(), maxEntries, direct);
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
        return get(root, key, 0);
    }

    public Node get(ByteBuffer key, int keyOffset) {
        return get(root, key, keyOffset);
    }

    private Node get(Node x, ByteBuffer key, int kOffset) {
        while (x != null) {
            int cmp = compareKeys(key, kOffset, x);
            if (cmp < 0) x = x.left;
            else if (cmp > 0) x = x.right;
            else return x;
        }
        return null;
    }

    private int compareRecord(ByteBuffer record, Node node) {
        int kOffset = Record2.KEY.offset(record);
        return compareKeys(record, kOffset, node);
    }

    private int compareKeys(ByteBuffer k1, int k1Offset, Node node) {
        return comparator.compare(k1, k1Offset, node.key, node.keyOffset);
    }

    public boolean contains(ByteBuffer key, int keyOffset) {
        return get(key, keyOffset) != null;
    }

    public void put(ByteBuffer record, int offset) {
        root = put(root, record, offset);
        root.color = BLACK;
    }

    private Node put(Node h, ByteBuffer record, int offset) {
        if (h == null) {
            Node node = allocateNode(record, offset);
            node.color = RED;
            node.size = 1;
            return node;
        }
        int cmp = compareRecord(record, h);
        if (cmp < 0) h.left = put(h.left, record, offset);
        else if (cmp > 0) {
            h.right = put(h.right, record, offset);
        } else { //equals, replace
            h.value = offset;
            h.len = Record2.sizeOf(record);
        }

        if (isRed(h.right) && !isRed(h.left)) h = rotateLeft(h);
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

    public ByteBuffer min() {
        if (isEmpty()) throw new NoSuchElementException("calls min() with empty symbol table");
        return min(root).key;
    }

    private Node min(Node x) {
        if (x.left == null) return x;
        else return min(x.left);
    }

    public ByteBuffer max() {
        if (isEmpty()) throw new NoSuchElementException("calls max() with empty symbol table");
        return max(root).key;
    }

    private Node max(Node x) {
        if (x.right == null) return x;
        else return max(x.right);
    }

    public Node floor(ByteBuffer key, int keyOffset) {
        if (isEmpty()) return null;
        return floor(root, key, keyOffset);
    }

    private Node floor(Node x, ByteBuffer key, int keyOffset) {
        if (x == null) return null;
        int cmp = compareKeys(key, keyOffset, x);
        if (cmp == 0) return x;
        if (cmp < 0) return floor(x.left, key, keyOffset);
        Node t = floor(x.right, key, keyOffset);
        if (t != null) return t;
        else return x;
    }

    public Node ceiling(ByteBuffer key, int keyOffset) {
        if (isEmpty()) return null;
        return ceiling(root, key, keyOffset);
    }

    private Node ceiling(Node x, ByteBuffer key, int keyOffset) {
        if (x == null) return null;
        int cmp = compareKeys(key, keyOffset, x);
        if (cmp == 0) return x;
        if (cmp > 0) return ceiling(x.right, key, keyOffset);
        Node t = ceiling(x.left, key, keyOffset);
        if (t != null) return t;
        else return x;
    }

    public ByteBuffer select(int k) {
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

    private Node allocateNode(ByteBuffer record, int offset) {
        Node node = nodePool.allocate();
        Record2.KEY.copyTo(record, node.key);
        node.key.flip();
        node.value = offset;
        node.len = Record2.sizeOf(record);
        return node;
    }

    public boolean isFull() {
        return size() >= maxEntries;
    }

    private static class NodePool implements Pool<Node> {
        private final Queue<Node> pool;
        private final ByteBuffer backingBuffer;
        private final int keySize;

        private NodePool(int keySize, int maxEntries, boolean direct) {
            this.keySize = keySize;
            this.pool = new ArrayDeque<>(maxEntries);
            this.backingBuffer = Buffers.allocate(keySize * maxEntries, direct);
        }

        @Override
        public Node allocate() {
            Node instance = pool.poll();
            if (instance == null) {
                int offset = backingBuffer.position();
                instance = new Node(backingBuffer, offset);
                Buffers.offsetPosition(backingBuffer, keySize);
            } else {
                clearNode(instance);
            }
            return instance;
        }

        @Override
        public void free(Node node) {
            pool.offer(node);
            node.size = 0;
        }

        private void clearNode(Node node) {
            node.key.clear();
            node.value = -1;
            node.len = 0;
            node.left = null;
            node.right = null;
            node.color = BLACK;
            node.size = 0;
        }

    }

}