package io.joshworks.ilog.lsm.tree;

import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.ilog.RecordUtils;
import io.joshworks.ilog.index.RowKey;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class RedBlackBSTTest {

    @Test
    public void iterator() {
        int items = 1500000;
        RedBlackBST tree = new RedBlackBST(RowKey.LONG);

        for (long i = 0; i < items; i++) {
            tree.put(RecordUtils.create(i, "value-" + i));
        }

        int count = 0;
        for (Node node : tree) {
            count++;
        }

        assertEquals(items, count);
        assertEquals(tree.size(), count);
    }

    @Test
    public void test() {
        RedBlackBST tree = new RedBlackBST(RowKey.LONG);

        int items = 10000;
        for (int i = 0; i < items; i++) {
            tree.put(RecordUtils.create(i, "value-" + i));
        }

        for (long i = 0; i < items; i++) {
            ByteBuffer key = Buffers.wrap(i);
            Node node = tree.get(key);
            assertNotNull("Failed on " + i, node);
            assertEquals("Failed on " + i, i, RecordUtils.longKey(node.record()));
        }
    }
}