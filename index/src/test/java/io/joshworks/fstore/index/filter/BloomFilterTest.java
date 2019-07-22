package io.joshworks.fstore.index.filter;

import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.fstore.testutils.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BloomFilterTest {

    private File testFolder;
    private BloomFilter filter;


    @Before
    public void setUp() {
        testFolder = FileUtils.testFolder();
        filter = BloomFilter.create(100, 0.01);
    }

    @After
    public void tearDown() {
        FileUtils.tryDelete(testFolder);
    }

    @Test
    public void contains() {
        filter.add(toBytes(1L));
        assertTrue(filter.contains(toBytes(1L)));
    }

    @Test
    public void loaded_filter_is_identical_in_size_and_set_bits() {
        filter.add(toBytes(1L));
        assertTrue(filter.contains(toBytes(1L)));
        assertFalse(filter.contains(toBytes(2L)));

        ByteBuffer data = ByteBuffer.allocate(4096);
        filter.writeTo(data);

        data.flip();

        BloomFilter loaded = BloomFilter.load(data);

        assertEquals(filter.hashes, loaded.hashes);
        assertEquals(filter.hashes.size(), loaded.hashes.size());
        assertEquals(filter.hashes.length(), loaded.hashes.length());
        assertEquals(filter.hashes.hashCode(), loaded.hashes.hashCode());
        assertArrayEquals(filter.hashes.toByteArray(), loaded.hashes.toByteArray());

        assertTrue(loaded.contains(toBytes(1L)));
        assertFalse(loaded.contains(toBytes(2L)));
    }

    private ByteBuffer toBytes(long value) {
        ByteBuffer bb = ByteBuffer.allocateDirect(Long.BYTES);
        Serializers.LONG.writeTo(value, bb);
        bb.flip();
        return bb;
    }

}