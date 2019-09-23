package io.joshworks.fstore.index.filter;

import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.io.buffers.ThreadLocalBufferPool;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.record.DataStream;
import io.joshworks.fstore.log.segment.footer.FooterMap;
import io.joshworks.fstore.log.segment.footer.FooterReader;
import io.joshworks.fstore.log.segment.footer.FooterWriter;
import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.fstore.core.util.FileUtils;
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

    private BloomFilter filter;
    private BufferPool bufferPool;
    private Storage storage;
    private FooterWriter writer;
    private FooterReader reader;
    private Codec codec = Codec.noCompression();

    @Before
    public void setUp() {
        File testFile = FileUtils.testFile();
        filter = BloomFilter.create(10000000, 0.01);

        bufferPool = new ThreadLocalBufferPool("buffer", Size.MB.ofInt(2), false);
        storage = Storage.create(testFile, StorageMode.MMAP, Size.MB.of(2));
        DataStream dataStream = new DataStream(bufferPool, storage);

        FooterMap map = new FooterMap();
        writer = new FooterWriter(dataStream, map);
        reader = new FooterReader(dataStream, map);
    }

    @After
    public void tearDown() {
        storage.delete();
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
        filter.writeTo(writer, codec, bufferPool);

        data.flip();

        BloomFilter loaded = BloomFilter.load(reader, codec, bufferPool);

        assertEquals(filter.hashes.size(), loaded.hashes.size());
        assertEquals(filter.hashes.length(), loaded.hashes.length());
        assertEquals(filter.hashes, loaded.hashes);
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