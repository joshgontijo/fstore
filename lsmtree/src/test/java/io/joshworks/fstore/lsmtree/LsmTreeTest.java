package io.joshworks.fstore.lsmtree;

import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.util.TestUtils;
import io.joshworks.fstore.log.CloseableIterator;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.appender.FlushMode;
import io.joshworks.fstore.lsmtree.sstable.entry.Entry;
import io.joshworks.fstore.serializer.Serializers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class LsmTreeTest {

    private LsmTree<Integer, String> lsmtree;
    private File testDirectory;
    private static final int FLUSH_THRESHOLD = 1000;


    @Before
    public void setUp() {
        testDirectory = TestUtils.testFolder();
        lsmtree = open(testDirectory);
    }

    private LsmTree<Integer, String> open(File dir) {
        return LsmTree.builder(dir, Serializers.INTEGER, Serializers.STRING)
                .flushThreshold(FLUSH_THRESHOLD)
                .sstableStorageMode(StorageMode.MMAP)
                .ssTableFlushMode(FlushMode.MANUAL)
                .open();
    }

    @After
    public void tearDown() {
        lsmtree.close();
        TestUtils.deleteRecursively(testDirectory);
    }

    @Test(expected = NullPointerException.class)
    public void key_must_be_provided_when_adding_an_entry() {
        lsmtree.put(null, "a");
    }

    @Test(expected = NullPointerException.class)
    public void value_must_be_provided_when_adding_an_entry() {
        lsmtree.put(1, null);
    }

    @Test(expected = NullPointerException.class)
    public void key_must_be_provided_when_deleting_an_entry() {
        lsmtree.remove(null);
    }

    @Test(expected = NullPointerException.class)
    public void key_must_be_provided_when_getting_an_entry() {
        lsmtree.get(null);
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
    public void data_is_restored_in_memtable_after_restart() {
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
    public void deleted_entries_are_not_returned_when_iterating() {
        int items = (int) (FLUSH_THRESHOLD * 3.5);
        for (int i = 0; i < items; i++) {
            lsmtree.put(i, String.valueOf(i));
        }
        //remove even keys
        for (int i = 0; i < items; i += 2) {
            lsmtree.remove(i);
        }

        try (CloseableIterator<Entry<Integer, String>> iterator = lsmtree.iterator(Direction.FORWARD)) {
            while (iterator.hasNext()) {
                Entry<Integer, String> entry = iterator.next();
                assertNotEquals(0, entry.key % 2);
            }
        }
    }

    @Test
    public void deleted_interleaving_entries_are_not_returned_when_iterating() {
        int items = (int) (FLUSH_THRESHOLD * 5.5);
        for (int i = 0; i < items; i++) {
            lsmtree.put(i, String.valueOf(i));
            if (i % 2 == 0) {
                lsmtree.remove(i);
            }
        }

        try (CloseableIterator<Entry<Integer, String>> iterator = lsmtree.iterator(Direction.FORWARD)) {
            while (iterator.hasNext()) {
                Entry<Integer, String> entry = iterator.next();
                assertTrue(entry.key % 2 != 0);
            }
        }
    }

    @Test
    public void iterator_deleted_entries() throws Exception {
        int items = FLUSH_THRESHOLD + (FLUSH_THRESHOLD / 2);
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
            }
        }
    }

    @Test
    public void can_get_without_reopening() {
        int items = FLUSH_THRESHOLD + (FLUSH_THRESHOLD / 2);
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
        int items = (int) (FLUSH_THRESHOLD * 2.5);
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
        int items = FLUSH_THRESHOLD + (FLUSH_THRESHOLD / 2);
        for (int i = 0; i < items; i++) {
            lsmtree.put(i, String.valueOf(i));
        }

        try (CloseableIterator<Entry<Integer, String>> iterator = lsmtree.iterator(Direction.BACKWARD, Range.start(9990))) {
            while (iterator.hasNext()) {
                Entry<Integer, String> entry = iterator.next();
            }
        }
    }

    @Test
    public void continuous_range_iterator() throws IOException {
        int items = (int) (FLUSH_THRESHOLD * 5.5);
        for (int i = 0; i < items; i++) {
            lsmtree.put(i, String.valueOf(i));
        }

        long start = System.currentTimeMillis();

//        for (int i = 0; i < Integer.MAX_VALUE; i++) {
//            try (CloseableIterator<Entry<Integer, String>> iterator = lsmtree.iterator(Direction.BACKWARD, Range.of(0, items))) {
//            }
//        }
//        System.out.println("1-TOOK " + (System.currentTimeMillis() - start) + "ms");

        start = System.currentTimeMillis();

        for (int i = 0; i < Integer.MAX_VALUE; i++) {
            try (CloseableIterator<Entry<Integer, String>> iterator = lsmtree.iterator(Direction.BACKWARD, Range.start(items + 1))) {
            }
        }
        System.out.println("2-TOOK " + (System.currentTimeMillis() - start) + "ms");


    }


}