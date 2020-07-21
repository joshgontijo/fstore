package io.joshworks.es.index.filter;

import io.joshworks.fstore.core.util.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BloomFilterTest {

    private File testFile;

    @Before
    public void setUp()  {
        testFile = TestUtils.testFile();
    }

    @Test
    public void reopen() {
        int items = 5_000_000;
        var bf = BloomFilter.createOrOpen(items, 0.07, testFile);

        var key = ByteBuffer.allocate(Integer.BYTES);
        for (int i = 0; i < items; i++) {
            key.clear().putInt(i).flip();
            bf.add(key);
            assertTrue(bf.contains(key));
        }
        for (int i = 0; i < items; i++) {
            key.clear().putInt(i).flip();
            assertTrue(bf.contains(key));
        }

        bf.close();
        bf = BloomFilter.createOrOpen(items, 0.07, testFile);
        for (int i = 0; i < items; i++) {
            key.clear().putInt(i).flip();
            assertTrue(bf.contains(key));
        }
    }

    @Test
    public void sample() {
        int items = 5_000_000;
        var bf = BloomFilter.createOrOpen(items, 0.05, testFile);

        long s = System.currentTimeMillis();
        var key = ByteBuffer.allocate(Long.BYTES + Integer.BYTES);
        for (int i = 0; i < items; i++) {
            key.clear().putLong(123).putInt(i).flip();
            bf.add(key);
            assertTrue(bf.contains(key));
        }
        System.out.println("ADD: " + (System.currentTimeMillis() - s));
        s = System.currentTimeMillis();
        for (int i = 0; i < items; i++) {
            key.clear().putLong(123).putInt(i).flip();
            assertTrue(bf.contains(key));
        }
        System.out.println("Contains: " + (System.currentTimeMillis() - s));

    }

    @Test
    public void empty() {
        int items = 2_000_000;
        var bf = BloomFilter.createOrOpen(items, 0.07, testFile);

        var key = ByteBuffer.allocate(Integer.BYTES);
        for (int i = 0; i < items; i++) {
            assertFalse(bf.contains(key));
        }
    }

    private static ByteBuffer bufferOf(long val) {
        return ByteBuffer.allocate(Long.BYTES).putLong(val).flip();
    }
}