package io.joshworks.fstore.lsmtree.sstable.index;

import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.fstore.testutils.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class IndexTest {

    private Index<Long> index;
    private File file;

    @Before
    public void setUp() {
        file = FileUtils.testFolder();
        index = new Index<>(file, "test", Serializers.LONG, "magic");
    }

    @After
    public void tearDown() {
        index.close();
        FileUtils.tryDelete(file);
    }

    @Test
    public void all_entries_are_returned() {
        int numItems = 10000000;
        for (long i = 0; i < numItems; i++) {
            index.add(i, i);
        }

        for (long i = 0; i < numItems; i++) {
            IndexEntry entry = index.get(i);
            assertNotNull(entry);
            assertEquals(i, entry.position);
            assertEquals(i, entry.key);
        }
    }

}