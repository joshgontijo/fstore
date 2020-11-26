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
    public void version() {
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
    public void get() {
    }
}