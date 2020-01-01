package io.joshworks.ilog;

import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.core.util.TestUtils;
import io.joshworks.fstore.serializer.Serializers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class IndexTest {

    private Index<Integer> index;
    private File testFile = TestUtils.testFile();
    private static int SPARENESS = 4096;

    @Before
    public void setUp() {
        index = new Index<>(testFile, Size.MB.of(10), Integer.BYTES, SPARENESS, Serializers.INTEGER);
    }

    @After
    public void tearDown() {
        index.delete();
    }

    @Test
    public void write() {

        int items = 100;
        TreeSet<Integer> set = new TreeSet<>();
        for (int i = 0; i < items; i += 5) {
            index.write(i, i);
            set.add(i);
        }

        for (int i = 0; i < items; i++) {
            Integer lower = set.floor(i);
            IndexEntry<Integer> entry = index.floor(i);

            assertNotNull(entry);
            assertEquals("Failed on " + i, lower, entry.key);
        }
    }
}