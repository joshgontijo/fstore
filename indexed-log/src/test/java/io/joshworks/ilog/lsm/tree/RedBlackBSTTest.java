//package io.joshworks.ilog.lsm.tree;
//
//import io.joshworks.fstore.core.io.buffers.Buffers;
//import io.joshworks.fstore.serializer.Serializers;
//import io.joshworks.ilog.Record;
//import io.joshworks.ilog.index.RowKey;
//import org.junit.Test;
//
//import java.nio.ByteBuffer;
//import java.util.concurrent.ThreadLocalRandom;
//
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertNotNull;
//
//public class RedBlackBSTTest {
//
//    @Test
//    public void iterator() {
//        int items = 1500000;
//        RedBlackBST tree = new RedBlackBST(RowKey.LONG, items, false);
//
//        for (int i = 0; i < items; i++) {
//            tree.put(create(i, "val-" + i), i);
//        }
//
//        int count = 0;
//        for (Node node : tree) {
//            count++;
//        }
//
//        assertEquals(items, count);
//    }
//
//    @Test
//    public void test() {
//        RedBlackBST tree = new RedBlackBST(RowKey.LONG, 30000, false);
//
//        ThreadLocalRandom random = ThreadLocalRandom.current();
//
//        int items = 10000;
//        for (int i = 0; i < items; i++) {
//            String val = "value-" + i;
//            tree.put(create(i, val), i);
//        }
//
//        for (int i = 0; i < items; i++) {
//            Node node = tree.get(keyOf(i));
//            assertNotNull("Failed on " + i, node);
//            assertEquals("Failed on " + i, i, node.offset());
//        }
//    }
//
//    private static ByteBuffer keyOf(long key) {
//        return ByteBuffer.allocate(Long.BYTES).putLong(key).flip();
//    }
//
//    private static ByteBuffer create(long key, String val) {
//        var kb = Buffers.allocate(8, false);
//        var vb = Buffers.allocate(64, false);
//        var dst = Buffers.allocate(256, false);
//
//        Serializers.LONG.writeTo(key, kb);
//        kb.flip();
//
//        Serializers.STRING.writeTo(val, vb);
//        vb.flip();
//
//        Record.create(kb, vb, dst);
//        dst.flip();
//        return dst;
//    }
//
//}