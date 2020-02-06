package io.joshworks.ilog.lsm;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.ilog.Record2;
import io.joshworks.ilog.index.KeyComparator;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class RedBlackTreeTest {

    @Test
    public void test() {
        RedBlackTree tree = new RedBlackTree(KeyComparator.LONG);

        for (int i = 0; i < 100; i++) {
            tree.put(create(i, "value-" + i), i);
        }

        for (int i = 0; i < 100; i++) {
            int val = tree.get(ByteBuffer.allocate(Long.BYTES).putLong(i).flip());
            assertEquals(i, val);
        }

        tree.clear();

        for (int i = 0; i < 100; i++) {
            tree.put(create(i, "value-" + i), i);
        }

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