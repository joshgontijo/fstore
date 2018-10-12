package io.joshworks.fstore.core.filter;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.utils.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BloomFilterTest {

    private File testFolder;
    private BloomFilter<Long> filter;


    @Before
    public void setUp() {
        testFolder = Utils.testFolder();
        filter = openFilter();

    }

    @After
    public void tearDown() {
        Utils.tryDelete(testFolder);
    }

    private BloomFilter<Long> openFilter() {
        return BloomFilter.openOrCreate(testFolder, "segmentA", 100, 0.01, BloomFilterHasher.murmur64(new LongSerializer()));
    }

    @Test
    public void contains() {
        filter.add(1L);
        assertTrue(filter.contains(1L));
    }

    @Test
    public void loaded_filter_is_identical_in_size_and_set_bits() {
        filter.add(1L);
        assertTrue(filter.contains(1L));
        assertFalse(filter.contains(2L));

        filter.write();

        BloomFilter<Long> loaded = openFilter();

        assertEquals(filter.hashes, loaded.hashes);
        assertEquals(filter.hashes.size(), loaded.hashes.size());
        assertEquals(filter.hashes.length(), loaded.hashes.length());
        assertEquals(filter.hashes.hashCode(), loaded.hashes.hashCode());
        assertTrue(Arrays.equals(filter.hashes.toByteArray(), loaded.hashes.toByteArray()));

        assertTrue(loaded.contains(1L));
        assertFalse(loaded.contains(2L));
    }

    private static class LongSerializer implements Serializer<Long> {

        @Override
        public ByteBuffer toBytes(Long data) {
           return ByteBuffer.allocate(Long.BYTES).putLong(data).flip();
        }

        @Override
        public void writeTo(Long data, ByteBuffer dest) {
            dest.putLong(data);
        }

        @Override
        public Long fromBytes(ByteBuffer buffer) {
            return buffer.getLong();
        }
    }

}