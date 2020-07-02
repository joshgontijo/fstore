package io.joshworks.ilog.lsm.filter;

import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BloomFilterTest {

    @Test
    public void name() {
        int items = 500_000;
        var bf = new BloomFilter(items, 0.07);

        long s = System.currentTimeMillis();
        var key = ByteBuffer.allocate(Integer.BYTES);
        for (int i = 0; i < items; i++) {
            key.clear().putInt(i).flip();
//            assertFalse("Failed on " + i, bf.contains(key));
            bf.add(key);
            assertTrue(bf.contains(key));
        }
        System.out.println("ADD: " + (System.currentTimeMillis() - s));
        s = System.currentTimeMillis();
        for (int i = 0; i < items; i++) {
            key.clear().putInt(i).flip();
            assertTrue(bf.contains(key));
        }
        System.out.println("Contains: " + (System.currentTimeMillis() - s));

    }

    @Test
    public void empty() {
        int items = 2_000_000;
        var bf = new BloomFilter(items, 0.07);

        var key = ByteBuffer.allocate(Integer.BYTES);
        for (int i = 0; i < items; i++) {
            assertFalse(bf.contains(key));
        }
    }

    private static ByteBuffer bufferOf(long val) {
        return ByteBuffer.allocate(Long.BYTES).putLong(val).flip();
    }
}