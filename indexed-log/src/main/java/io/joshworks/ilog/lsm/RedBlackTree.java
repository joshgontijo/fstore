package io.joshworks.ilog.lsm;


import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.Pool;
import io.joshworks.ilog.Record2;
import io.joshworks.ilog.index.KeyComparator;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Queue;

public class RedBlackTree {

    private static final int RED = 0;
    private static final int BLACK = 1;

    //    private final Node nil = new Node(null);
    private final NodePool nodePool;
    private Node root = null;
    private final KeyComparator comparator;

    public RedBlackTree(KeyComparator comparator) {
        this.comparator = comparator;
        this.nodePool = new NodePool(comparator.keySize());
    }

    private class Node {

        private ByteBuffer key;
        private int value;
        private int color = BLACK;
        private Node left = null;
        private Node right = null;
        private Node parent = null;

        Node(ByteBuffer key) {
            this.key = key;
        }

        public Node parent() {
            if (parent == null) {
                parent = nodePool.allocate(); // allocate empty
            }
            return parent;
        }

        public Node left() {
            if (left == null) {
                left = nodePool.allocate(); // allocate empty
            }
            return left;
        }

        public Node right() {
            if (right == null) {
                right = nodePool.allocate(); // allocate empty
            }
            return right;
        }
    }

    public void put(ByteBuffer record, int arrayPos) {
        Node node = allocateNode(record, arrayPos);
        Node temp = root;
        if (root == null) {
            root = node;
            node.color = BLACK;
            removeNode(node.parent);
            node.parent = null;
            return;
        }

        node.color = RED;
        while (true) {
            int cmpr = comparator.compare(node.key, temp.key);
            if (cmpr < 0) {
                if (temp.left == null) {
                    temp.left = node;
                    node.parent = temp;
                    break;
                } else {
                    temp = temp.left;
                }
            } else {
                if (temp.right == null) {
                    temp.right = node;
                    node.parent = temp;
                    break;
                } else {
                    temp = temp.right;
                }
            }
        }
        fixTree(node);
    }

    public int get(ByteBuffer key) {
        if (root == null) {
            return 0;
        }
        Node found = doFind(key, root);
        if (found == null) {
            return 0;
        }
        return found.value;
    }


    private Node findNode(Node findNode, Node node) {
        if (root == null) {
            return null;
        }

        return doFind(findNode.key, root);
    }

    private Node doFind(ByteBuffer key, Node node) {
        int cmpr = comparator.compare(key, node.key);

        if (cmpr < 0) {
            if (node.left != null) {
                return doFind(key, node.left);
            }
        } else if (cmpr > 0) {
            if (node.right != null) {
                return doFind(key, node.right);
            }
        } else {
            return node;
        }
        return null;
    }

    //Takes as argument the newly inserted node
    private void fixTree(Node node) {
        while (node.parent().color == RED) {
            Node uncle = null;
            if (node.parent == node.parent().parent().left) {
                uncle = node.parent().parent().right;

                if (uncle != null && uncle.color == RED) {
                    node.parent().color = BLACK;
                    uncle.color = BLACK;
                    node.parent().parent().color = RED;
                    node = node.parent().parent;
                    continue;
                }
                if (node == node.parent().right) {
                    //Double rotation needed
                    node = node.parent;
                    rotateLeft(node);
                }
                node.parent().color = BLACK;
                node.parent().parent().color = RED;
                //if the "else if" code hasn't executed, this
                //is a case where we only need a single rotation
                rotateRight(node.parent().parent);
            } else {
                uncle = node.parent().parent().left;
                if (uncle != null && uncle.color == RED) {
                    node.parent().color = BLACK;
                    uncle.color = BLACK;
                    node.parent().parent().color = RED;
                    node = node.parent().parent;
                    continue;
                }
                if (node == node.parent().left) {
                    //Double rotation needed
                    node = node.parent;
                    rotateRight(node);
                }
                node.parent().color = BLACK;
                node.parent().parent().color = RED;
                //if the "else if" code hasn't executed, this
                //is a case where we only need a single rotation
                rotateLeft(node.parent().parent);
            }
        }
        root.color = BLACK;
    }

    void rotateLeft(Node node) {
        if (node.parent != null) {
            if (node == node.parent().left) {
                node.parent().left = node.right;
            } else {
                node.parent().right = node.right;
            }
            node.right().parent = node.parent;
            node.parent = node.right;
            if (node.right().left != null) {
                node.right().left().parent = node;
            }
            node.right = node.right().left;
            node.parent().left = node;
        } else {//Need to rotate root
            Node right = root.right;
            root.right = right.left;
            right.left().parent = root;
            root.parent = right;
            right.left = root;
            right.parent = null;
            root = right;
        }
    }

    void rotateRight(Node node) {
        if (node.parent != null) {
            if (node == node.parent().left) {
                node.parent().left = node.left;
            } else {
                node.parent().right = node.left;
            }

            node.left().parent = node.parent;
            node.parent = node.left;
            if (node.left().right != null) {
                node.left().right().parent = node;
            }
            node.left = node.left().right;
            node.parent().right = node;
        } else {//Need to rotate root
            Node left = root.left;
            root.left = root.left().right;
            left.right().parent = root;
            root.parent = left;
            left.right = root;
            left.parent = null;
            root = left;
        }
    }

    //Deletes whole tree
    void clear() {
        nodePool.clear();
        root = null;
    }

    //Deletion Code .

    //This operation doesn't care about the new Node's connections
    //with previous node's left and right. The caller has to take care
    //of that.
    void transplant(Node target, Node with) {
        if (target.parent == null) {
            root = with;
        } else if (target == target.parent().left) {
            target.parent().left = with;
        } else
            target.parent().right = with;
        with.parent = target.parent;
    }

    boolean delete(Node z) {
        if ((z = findNode(z, root)) == null) return false;
        Node x;
        Node y = z; // temporary reference y
        int y_original_color = y.color;

        if (z.left == null) {
            x = z.right;
            transplant(z, z.right);
        } else if (z.right == null) {
            x = z.left;
            transplant(z, z.left);
        } else {
            y = treeMinimum(z.right);
            y_original_color = y.color;
            x = y.right;
            if (y.parent == z)
                x.parent = y;
            else {
                transplant(y, y.right);
                y.right = z.right;
                y.right().parent = y;
            }
            transplant(z, y);
            y.left = z.left;
            y.left().parent = y;
            y.color = z.color;
        }
        if (y_original_color == BLACK)
            deleteFixup(x);
        return true;
    }

    void deleteFixup(Node x) {
        while (x != root && x.color == BLACK) {
            if (x == x.parent().left) {
                Node w = x.parent().right;
                if (w.color == RED) {
                    w.color = BLACK;
                    x.parent().color = RED;
                    rotateLeft(x.parent);
                    w = x.parent().right;
                }
                if (w.left().color == BLACK && w.right().color == BLACK) {
                    w.color = RED;
                    x = x.parent;
                    continue;
                } else if (w.right().color == BLACK) {
                    w.left().color = BLACK;
                    w.color = RED;
                    rotateRight(w);
                    w = x.parent().right;
                }
                if (w.right().color == RED) {
                    w.color = x.parent().color;
                    x.parent().color = BLACK;
                    w.right().color = BLACK;
                    rotateLeft(x.parent);
                    x = root;
                }
            } else {
                Node w = x.parent().left;
                if (w.color == RED) {
                    w.color = BLACK;
                    x.parent().color = RED;
                    rotateRight(x.parent);
                    w = x.parent().left;
                }
                if (w.right().color == BLACK && w.left().color == BLACK) {
                    w.color = RED;
                    x = x.parent;
                    continue;
                } else if (w.left().color == BLACK) {
                    w.right().color = BLACK;
                    w.color = RED;
                    rotateLeft(w);
                    w = x.parent().left;
                }
                if (w.left().color == RED) {
                    w.color = x.parent().color;
                    x.parent().color = BLACK;
                    w.left().color = BLACK;
                    rotateRight(x.parent);
                    x = root;
                }
            }
        }
        x.color = BLACK;
    }

    Node treeMinimum(Node subTreeRoot) {
        while (subTreeRoot.left != null) {
            subTreeRoot = subTreeRoot.left;
        }
        return subTreeRoot;
    }

    private Node allocateNode(ByteBuffer record, int arrayPosition) {
        Node node = nodePool.allocate();
        Record2.KEY.copyTo(record, node.key);
        node.key.flip();
        node.value = arrayPosition;
        return node;
    }


    private void removeNode(Node node) {
        if (node != null) {
            nodePool.free(node);
        }
    }

    private class NodePool implements Pool<Node> {
        private final Queue<Node> pool = new ArrayDeque<>();
        private final Queue<Node> inUse = new ArrayDeque<>();
        private int keySize;

        private NodePool(int keySize) {
            this.keySize = keySize;
        }

        @Override
        public Node allocate() {
            Node instance = pool.poll();
            if (instance == null) {
                instance = new Node(Buffers.allocate(keySize, false));
            } else {
                clearNode(instance);
            }
            inUse.add(instance);
            return instance;
        }

        @Override
        public void free(RedBlackTree.Node element) {
            pool.offer(element);
        }

        private void clearNode(Node node) {
            node.key.clear();
            node.value = -1;
            node.parent = null;
            node.left = null;
            node.right = null;
        }

        private void clear() {
            pool.addAll(inUse);
            inUse.clear();
        }

    }


}