package io.joshworks.es2.sstable;

import io.joshworks.es2.StreamHasher;
import io.joshworks.es2.index.IndexEntry;
import io.joshworks.fstore.core.util.Iterators;
import io.joshworks.fstore.core.util.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class SSTableTest {

    private File dataFile;
    private File indexFile;

    @Before
    public void open() {
        dataFile = TestUtils.testFile();
        indexFile = TestUtils.testFile();
    }

    @After
    public void tearDown() {
        TestUtils.deleteRecursively(dataFile);
        TestUtils.deleteRecursively(indexFile);
    }

    @Test
    public void version_single() {
        String stream = "stream-1";
        int version = 0;
        ByteBuffer data = EventSerializer.serialize(stream, "type-1", version, "data", 0);
        SSTable sstable = SSTable.create(dataFile, indexFile, Iterators.of(data));

        long streamHash = StreamHasher.hash(stream);
        IndexEntry ie = sstable.get(streamHash, version);
        assertNotNull(ie);
        assertEquals(streamHash, ie.stream());
        assertEquals(version, ie.version());
    }


    @Test
    public void version_many() {
        String stream = "stream-1";
        int items = 10000;

        List<ByteBuffer> events = IntStream.range(0, items)
                .mapToObj(v -> EventSerializer.serialize(stream, "type-1", v, "data", 0))
                .collect(Collectors.toList());

        SSTable sstable = SSTable.create(dataFile, indexFile, events.iterator());

        for (int version = 0; version < items; version++) {
            long streamHash = StreamHasher.hash(stream);
            IndexEntry ie = sstable.get(streamHash, version);
            assertNotNull("Failed on " + version, ie);
            assertEquals(streamHash, ie.stream());
            assertEquals(0, ie.version());
        }


    }

}