package io.joshworks.fstore.index;

import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.log.record.DataStream;
import io.joshworks.fstore.log.segment.footer.FooterMap;
import io.joshworks.fstore.log.segment.footer.FooterReader;
import io.joshworks.fstore.log.segment.footer.FooterWriter;
import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.fstore.testutils.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class IndexTest {

    private Storage storage;
    private DataStream stream;

    @Before
    public void setUp() {
        storage = Storage.create(FileUtils.testFile(), StorageMode.RAF, 4096);
        stream = new DataStream(new BufferPool(false), storage, 1, 4096);
    }

    @After
    public void cleanup() throws IOException {
        storage.flush();
        storage.delete();
    }

    @Test
    public void can_query_in_memory_items_without_flushing_to_disk() {
        int items = 100000;

        FooterMap map = new FooterMap();

        FooterReader reader = new FooterReader(stream, map);
        SparseIndex<String> index = SparseIndex.builder(Serializers.VSTRING, reader).build();

        for (int i = 0; i < items; i++) {
            index.add(String.valueOf(i), i);
        }

        for (int i = 0; i < items; i++) {
            IndexEntry<String> found = index.get(String.valueOf(i));
            assertNotNull(found);
            assertEquals(i, found.position);
        }
    }

    @Test
    public void can_query_in_memory_items_after_flushing_to_disk() {
        int items = 100000;

        FooterMap map = new FooterMap();

        FooterReader reader = new FooterReader(stream, map);
        SparseIndex<String> index = SparseIndex.builder(Serializers.VSTRING, reader).build();

        for (int i = 0; i < items; i++) {
            index.add(String.valueOf(i), i);
        }

        FooterWriter writer = new FooterWriter(stream, map);
        index.writeTo(writer);

        index = SparseIndex.builder(Serializers.VSTRING, reader).build();
        index.load();


        long start = System.currentTimeMillis();
        for (int i = 0; i < items; i++) {
            IndexEntry<String> found = index.get(String.valueOf(i));
            assertNotNull(found);
            assertEquals(i, found.position);
        }
        long end = System.currentTimeMillis();
        System.out.println((end - start));
    }
}