package io.joshworks.es.index.tree;

import io.joshworks.es.index.IndexEntry;
import io.joshworks.es.index.IndexKey;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class RedBlackBSTTest {

    private static int MAX_ENTRIES = 1500000;
    private static long DEFAULT_STREAM = 123;

    @Test
    public void iterator() {
        int items = MAX_ENTRIES;
        RedBlackBST tree = new RedBlackBST(MAX_ENTRIES);

        for (int i = 0; i < items; i++) {
            tree.put(create(i));
            assertEquals(i, tree.entries());
        }

        int count = 0;
        for (Node node : tree) {
            count++;
        }

        assertEquals(items, count);
        assertEquals(tree.entries(), count);
    }

    @Test
    public void test() {
        RedBlackBST tree = new RedBlackBST(MAX_ENTRIES);

        for (int i = 0; i < MAX_ENTRIES; i++) {
            tree.put(create(i));
        }

        for (int i = 0; i < MAX_ENTRIES; i++) {
            Node node = tree.get(key(i));
            assertNotNull("Failed on " + i, node);
            assertEquals("Failed on " + i, i, node.version);
        }
    }

    private IndexEntry create(int version) {
        return new IndexEntry(DEFAULT_STREAM, version, 1);
    }

    private IndexKey key(int version) {
        return new IndexKey(DEFAULT_STREAM, version);
    }
}