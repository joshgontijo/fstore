package io.joshworks.ilog.lsm.tree;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.ilog.Record;
import io.joshworks.ilog.index.KeyComparator;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class RedBlackBSTTest {

    @Test
    public void iterator() {
        int items = 1500000;
        RedBlackBST tree = new RedBlackBST(KeyComparator.LONG, items, false);

        for (int i = 0; i < items; i++) {
            tree.put(create(i, "val-" + i), i);
        }

        int count = 0;
        for (Node node : tree) {
            count++;
        }

        assertEquals(items, count);
    }

    @Test
    public void test() {
        RedBlackBST tree = new RedBlackBST(KeyComparator.LONG, 30000, false);

        ThreadLocalRandom random = ThreadLocalRandom.current();

        int items = 100;
        for (int i = 0; i < items; i++) {
            tree.put(create(i, "value-" + i), i);
        }

        for (int i = 0; i < items; i++) {
            Node node = tree.get(ByteBuffer.allocate(Long.BYTES).putLong(i).flip());
            assertNotNull("Failed on " + i, node);
            assertEquals("Failed on " + i, i, node.offset());
        }

//        tree.clear();
//
//        for (int i = 0; i < 100; i++) {
//            tree.put(create(i, "value-" + i), i);
//        }

    }

    public static ByteBuffer create(long key, String val) {
        return create(key, Serializers.LONG, val, Serializers.STRING);
    }

    public static <K, V> ByteBuffer create(K key, Serializer<K> ks, V value, Serializer<V> vs) {
        var kb = Buffers.allocate(128, false);
        var vb = Buffers.allocate(64, false);
        var dst = Buffers.allocate(256, false);

        ks.writeTo(key, kb);
        kb.flip();

        vs.writeTo(value, vb);
        vb.flip();

        Record.create(kb, vb, dst);
        dst.flip();
        return dst;
    }

}