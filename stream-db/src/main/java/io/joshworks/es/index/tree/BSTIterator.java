package io.joshworks.es.index.tree;

import java.util.Iterator;
import java.util.Stack;

public class BSTIterator implements Iterator<Node> {
    Stack<Node> stack;

    BSTIterator(Node root) {
        stack = new Stack<>();
        while (root != null) {
            stack.push(root);
            root = root.left;
        }
    }

    public boolean hasNext() {
        return !stack.isEmpty();
    }

    public Node next() {
        Node node = stack.pop();
        Node result = node;
        if (node.right != null) {
            node = node.right;
            while (node != null) {
                stack.push(node);
                node = node.left;
            }
        }
        return result;
    }

}
