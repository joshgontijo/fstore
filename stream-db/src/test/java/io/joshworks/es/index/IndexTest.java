package io.joshworks.es.index;


import io.joshworks.fstore.core.util.TestUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class IndexTest {

    private static int MAX_ENTRIES = 1000000;
    private Index index;
    private File root;

    @Before
    public void setUp() {
        root = TestUtils.testFolder();
        index = open();
    }

    public Index open() {
        return new Index(root, MAX_ENTRIES, 4096);
    }

    @Test
    public void entries() {
        long stream = 123;
        int items = MAX_ENTRIES;
        for (int i = 0; i < items; i++) {
            index.append(new IndexEntry(stream, i,  222));
        }

        assertEquals(items, index.entries());
    }

    @Test
    public void findFromMemTable() {
        long stream = 123;
        for (int i = 0; i < MAX_ENTRIES; i++) {
            index.append(new IndexEntry(stream, i,  222));
        }

        for (int i = 0; i < MAX_ENTRIES; i++) {
            IndexEntry entry = index.get(new IndexKey(stream, i));
            assertNotNull(entry);

            assertEquals(stream, entry.stream());
            assertEquals(i, entry.version());
            assertEquals(222, entry.logAddress());
        }
    }

    @Test
    public void findFromSegment() {
        long stream = 123;
        for (int i = 0; i < MAX_ENTRIES; i++) {
            index.append(new IndexEntry(stream, i,  222));
        }
        index.flush();

        for (int i = 0; i < MAX_ENTRIES; i++) {
            IndexEntry entry = index.get(new IndexKey(stream, i));
            assertNotNull(entry);

            assertEquals(stream, entry.stream());
            assertEquals(i, entry.version());
            assertEquals(222, entry.logAddress());
        }
    }

    @Test
    public void reopenFind() {
        long stream = 123;
        for (int i = 0; i < MAX_ENTRIES; i++) {
            index.append(new IndexEntry(stream, i,  222));
        }
        index.flush();
        index.close();
        index = open();

        for (int i = 0; i < MAX_ENTRIES; i++) {
            IndexEntry entry = index.get(new IndexKey(stream, i));
            assertNotNull(entry);

            assertEquals(stream, entry.stream());
            assertEquals(i, entry.version());
            assertEquals(222, entry.logAddress());
        }
    }

    @Test
    public void findFromMultipleSegments() {
        int segments = 5;
        long stream = 123;
        int items = MAX_ENTRIES * segments;

        for (int i = 0; i < items; i++) {
            index.append(new IndexEntry(stream, i,  222));
        }
        index.flush();

        for (int i = 0; i < items; i++) {
            IndexEntry entry = index.get(new IndexKey(stream, i));
            assertNotNull(entry);

            assertEquals(stream, entry.stream());
            assertEquals(i, entry.version());
            assertEquals(222, entry.logAddress());
        }
    }
}