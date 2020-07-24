package io.joshworks.es.index;

import io.joshworks.es.index.btree.BTreeIndexSegment;
import io.joshworks.fstore.core.util.Memory;
import io.joshworks.fstore.core.util.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class BTreeIndexSegmentTest {

    private static final int MAX_ENTRIES = 4000000;
    private BTreeIndexSegment index;
    private File testFile;

    @Before
    public void setUp() {
        testFile = TestUtils.testFile();
        index = new BTreeIndexSegment(testFile, MAX_ENTRIES, Memory.PAGE_SIZE);
    }

    @After
    public void tearDown() {
        index.delete();
    }

    @Test
    public void name() {
        int items = MAX_ENTRIES;
        long stream = 123;

        for (int i = 0; i < items; i++) {
            index.append(stream, i, 1);
        }
        index.complete();

        for (int i = 0; i < items; i++) {
            IndexEntry found = index.find(new IndexKey(stream, i), IndexFunction.EQUALS);
            assertNotNull("Failed on " + i, found);
            assertEquals("Failed on " + i, stream, found.stream());
            assertEquals("Failed on " + i, i, found.version());
        }


    }
}