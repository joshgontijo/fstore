package io.joshworks.fstore.lsmtree;

import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.index.Range;
import io.joshworks.fstore.log.CloseableIterator;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.appender.FlushMode;
import io.joshworks.fstore.lsmtree.sstable.Entry;
import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.fstore.testutils.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class LsmTreeTest {

    private LsmTree<Integer, String> lsmtree;
    private File testDirectory;


    @Before
    public void setUp() {
        testDirectory = FileUtils.testFolder();
        lsmtree = open(testDirectory);
    }

    private LsmTree<Integer, String> open(File dir) {
        return LsmTree.builder(dir, Serializers.INTEGER, Serializers.STRING)
                .flushThreshold(100)
                .disableTransactionLog()
                .sstableStorageMode(StorageMode.MMAP)
                .ssTableFlushMode(FlushMode.MANUAL)
                .open();
    }

    @After
    public void tearDown() {
        lsmtree.close();
        FileUtils.tryDelete(testDirectory);
    }

    @Test
    public void put_get() {
        lsmtree.put(1, "a");
        lsmtree.put(2, "b");
        lsmtree.put(3, "c");

        assertEquals("a", lsmtree.get(1));
        assertEquals("b", lsmtree.get(2));
        assertEquals("c", lsmtree.get(3));
    }

    @Test
    public void restart() {
        lsmtree.put(1, "a");
        lsmtree.put(2, "b");
        lsmtree.put(3, "c");

        lsmtree.close();

        lsmtree = open(testDirectory);

        assertNotNull(lsmtree.get(1));
        assertNotNull(lsmtree.get(2));
        assertNotNull(lsmtree.get(3));

    }

    @Test
    public void update() {
        lsmtree.put(1, "a");
        lsmtree.put(1, "b");
        lsmtree.put(1, "c");

        assertEquals("c", lsmtree.get(1));
    }

    @Test
    public void delete() {
        lsmtree.put(1, "a");
        lsmtree.put(2, "b");
        lsmtree.remove(2);

        assertEquals("a", lsmtree.get(1));
        assertNull(lsmtree.get(2));
    }

    @Test
    public void iterator_deleted_entries() throws Exception {
        int items = 10000;
        for (int i = 0; i < items; i++) {
            lsmtree.put(i, String.valueOf(i));
        }

        for (int i = items - 100; i < items; i++) {
            lsmtree.remove(i);
        }

        for (int i = 0; i < items; i++) {
            lsmtree.remove(i);
        }

        try (CloseableIterator<Entry<Integer, String>> iterator = lsmtree.iterator(Direction.FORWARD)) {
            while (iterator.hasNext()) {
                Entry<Integer, String> entry = iterator.next();
                System.out.println(entry);
            }
        }
    }

    @Test
    public void can_iterator_over_entries_without_reopening() {
        int items = 10000;
        for (int i = 0; i < items; i++) {
            lsmtree.put(i, String.valueOf(i));
        }

        for (int i = 0; i < items; i++) {
            String val = lsmtree.get(i);
            assertEquals("Failed on " + i, String.valueOf(i), val);
        }
    }

    @Test
    public void can_iterator_over_entries_after_reopening() {
        int items = 10000;
        for (int i = 0; i < items; i++) {
            lsmtree.put(i, String.valueOf(i));
        }

        lsmtree.close();

        lsmtree = open(testDirectory);

        for (int i = 0; i < items; i++) {
            String val = lsmtree.get(i);
            assertEquals("Failed on " + i, String.valueOf(i), val);
        }
    }

    @Test
    public void range_iterator() throws IOException {
        int items = 10000;
        for (int i = 0; i < items; i++) {
            lsmtree.put(i, String.valueOf(i));
        }

        try(CloseableIterator<Entry<Integer, String>> iterator = lsmtree.iterator(Direction.BACKWARD, Range.startingWith(9990))) {
            while (iterator.hasNext()) {
                Entry<Integer, String> entry = iterator.next();
                System.out.println(entry);
            }
        }
    }

    @Test
    public void continuous_iterator() throws IOException {
        lsmtree.put(1, "");

        try(CloseableIterator<Entry<Integer, String>> iterator = lsmtree.iterator(Direction.FORWARD)) {
            Entry<Integer, String> entry = iterator.next();
            assertEquals(Integer.valueOf(1), entry.key);

            lsmtree.put(2, "");
            assertTrue(iterator.hasNext());
            assertEquals(Integer.valueOf(2), iterator.next().key);
        }
    }

}