package io.joshworks.ilog.lsm;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.ilog.Record2;
import io.joshworks.ilog.index.KeyComparator;
import io.joshworks.ilog.lsm.tree.Node;
import io.joshworks.ilog.lsm.tree.RedBlackBST;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class RedBlackBSTTest {

    @Test
    public void test() {
        RedBlackBST tree = new RedBlackBST(KeyComparator.LONG, 30000, false);

        ThreadLocalRandom random = ThreadLocalRandom.current();

        List<Integer> lengths = new ArrayList<>();
        int items = 100;
        for (int i = 0; i < items; i++) {
            int v = random.nextInt();
            String val = "value-" + v;
            lengths.add(val.length());
            tree.put(create(v, val), i);
        }


        Iterator<Node> it = tree.iterator();
        while(it.hasNext()) {
            Node next = it.next();
            System.out.println(next.key.getLong(0));
        }


        for (int i = 0; i < items; i++) {
            Node node = tree.get(ByteBuffer.allocate(Long.BYTES).putLong(i).flip());
            assertNotNull(node);
            assertEquals("Failed on " + i, i, node.offset());
            assertEquals("Failed on " + i, lengths.get(i), Integer.valueOf(node.len()));
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

        Record2.create(kb, vb, dst);
        dst.flip();
        return dst;
    }

}