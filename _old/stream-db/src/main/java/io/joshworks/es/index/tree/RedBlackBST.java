package io.joshworks.es.index.tree;

import io.joshworks.es.index.IndexEntry;
import io.joshworks.es.index.IndexFunction;
import io.joshworks.es.index.IndexKey;
import io.joshworks.fstore.core.util.ObjectPool;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * An fixed size, {@link ByteBuffer} backed tree
 */
public class RedBlackBST implements Iterable<Node> {
    private static final boolean RED = true;
    private static final boolean BLACK = false;
    private final int maxEntries;
    private final ObjectPool<Node> nodePool;
    private Node root;

    public RedBlackBST(int maxEntries) {
        this.maxEntries = maxEntries;
        this.nodePool = new ObjectPool<>(maxEntries + 10, Node::new); //some extra entries for root
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

    private int entries(Node x) {
        if (x == null) return 0;
        return x.size;
    }

    public int entries() {
        return entries(root);
    }

    public boolean isEmpty() {
        return root == null;
    }

    public Node apply(IndexKey key, IndexFunction fn) {
        if (IndexFunction.CEILING.equals(fn)) {
            return ceiling(root, key);
        }
        if (IndexFunction.EQUALS.equals(fn)) {
            return get(root, key);
        }
        if (IndexFunction.FLOOR.equals(fn)) {
            return floor(root, key);
        }
        //TODO ADD LOWER AND HIGHER
        throw new UnsupportedOperationException("FUNCTION NOT SUPPORTED");
    }

    public Node get(IndexKey key) {
        return get(root, key);
    }

    private Node get(Node x, IndexKey key) {
        while (x != null) {
            int cmp = compare(x, key);
            if (cmp < 0) x = x.left;
            else if (cmp > 0) x = x.right;
            else return x;
        }
        return null;
    }

    public void put(IndexEntry ie) {
        if (isFull()) {
            throw new IllegalStateException("Table is full");
        }

        root = put(root, ie);
        root.color = BLACK;
    }

    private int compare(Node node, IndexKey key) {
        return IndexKey.compare(key, node.stream, node.version);
    }

    private int compare(Node node, IndexEntry key) {
        return IndexEntry.compare(key, node.stream, node.version);
    }

    private Node put(Node h, IndexEntry ie) {
        if (h == null) {
            Node node = allocateNode(ie);
            node.color = RED;
            node.size = 1;
            return node;
        }
        int cmp = compare(h, ie);
        if (cmp < 0) h.left = put(h.left, ie);
        else if (cmp > 0) {
            h.right = put(h.right, ie);
        } else { //equals, replace
            h.update(ie);
        }

        if (isRed(h.right) && !isRed(h.left)) h = rotateLeft(h);
        return checkRotateOrFlip(h);
    }

    private Node checkRotateOrFlip(Node h) {
        if (isRed(h.left) && isRed(h.left.left)) h = rotateRight(h);
        if (isRed(h.left) && isRed(h.right)) flipColors(h);
        h.size = entries(h.left) + entries(h.right) + 1;
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
        h.size = entries(h.left) + entries(h.right) + 1;
        return x;
    }

    private Node rotateLeft(Node h) {
        Node x = h.right;
        h.right = x.left;
        x.left = h;
        x.color = x.left.color;
        x.left.color = RED;
        x.size = h.size;
        h.size = entries(h.left) + entries(h.right) + 1;
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

    private Node floor(Node x, IndexKey key) {
        if (x == null) return null;
        int cmp = compare(x, key);
        if (cmp == 0) return x;
        if (cmp < 0) return floor(x.left, key);
        Node t = floor(x.right, key);
        if (t != null) return t;
        else return x;
    }


    private Node ceiling(Node x, IndexKey key) {
        if (x == null) return null;
        int cmp = compare(x, key);
        if (cmp == 0) return x;
        if (cmp > 0) return ceiling(x.right, key);
        Node t = ceiling(x.left, key);
        if (t != null) return t;
        else return x;
    }


    private Node allocateNode(IndexEntry ie) {
        Node node = nodePool.allocate();
        node.init(ie);
        node.left = null;
        node.right = null;
        node.size = 0;
        node.color = BLACK;
        return node;
    }

    public boolean isFull() {
        return entries() >= maxEntries;
    }

}