package io.joshworks.es2.index;

import io.joshworks.fstore.core.util.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BIndexTest {

    private File dataFile;
    private BIndex index;

    @Before
    public void open() {
        dataFile = TestUtils.testFile();
    }

    @After
    public void tearDown() {
        TestUtils.deleteRecursively(dataFile);
    }

    @Test
    public void filter_contain() {
        int streams = 100;
        try (BIndex.Writer writer = BIndex.writer(dataFile, 100, 0.001)) {
            for (long stream = 0; stream < streams; stream++) {
                writer.stampEntry(stream, 0);
            }
        }
        try (var index = BIndex.open(dataFile)) {
            for (long stream = 0; stream < streams; stream++) {
                assertTrue("Failed on " + stream, index.contains(stream, 0));
            }

            for (long stream = streams; stream < streams * 2; stream++) {
                assertFalse("Failed on " + stream, index.contains(stream, 0));
            }
        }
    }

    @Test
    public void dense_entries() {
        int streams = 100;
        try (BIndex.Writer writer = BIndex.writer(dataFile, 100, 0.001)) {
            for (long stream = 0; stream < streams; stream++) {
                writer.stampEntry(stream, 0);
            }
        }
        try (var index = BIndex.open(dataFile)) {
            assertEquals(streams, index.denseEntries());
        }
    }
}