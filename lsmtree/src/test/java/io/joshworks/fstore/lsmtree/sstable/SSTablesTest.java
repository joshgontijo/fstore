package io.joshworks.fstore.lsmtree.sstable;

import io.joshworks.fstore.codec.snappy.SnappyCodec;
import io.joshworks.fstore.core.cache.Cache;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.util.FileUtils;
import io.joshworks.fstore.core.util.Memory;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.index.Range;
import io.joshworks.fstore.log.CloseableIterator;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.appender.FlushMode;
import io.joshworks.fstore.log.iterators.Iterators;
import io.joshworks.fstore.log.segment.block.Block;
import io.joshworks.fstore.serializer.Serializers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static io.joshworks.fstore.lsmtree.sstable.Entry.NO_MAX_AGE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class SSTablesTest {

    private static final int FLUSH_THRESHOLD = 1000;
    private SSTables<Integer, String> sstables;
    private File testDirectory;

    @Before
    public void setUp() {
        testDirectory = FileUtils.testFolder();
        sstables = open(testDirectory);
    }

    private SSTables<Integer, String> open(File dir) {
        return new SSTables<>(
                dir,
                Serializers.INTEGER,
                Serializers.STRING,
                "test",
                Size.MB.ofInt(5),
                FLUSH_THRESHOLD,
                StorageMode.MMAP,
                FlushMode.MANUAL,
                Block.vlenBlock(),
                new SSTableCompactor<>(NO_MAX_AGE),
                NO_MAX_AGE,
                new SnappyCodec(),
                1000000,
                0.01,
                Memory.PAGE_SIZE,
                Cache.softCache());
    }

    @After
    public void tearDown() {
        sstables.close();
        FileUtils.tryDelete(testDirectory);
    }

    @Test
    public void get_returns_items_from_memTable() {
        int items = FLUSH_THRESHOLD - 1;
        for (int i = 0; i < items; i++) {
            sstables.add(Entry.add(i, String.valueOf(i)));
        }

        for (int i = 0; i < items; i++) {
            Entry<Integer, String> entry = sstables.get(i);
            assertNotNull(entry);
            assertEquals(Integer.valueOf(i), entry.key);
            assertEquals(String.valueOf(i), entry.value);
        }
    }

    @Test
    public void iterator_returns_items_from_memTable() {
        int items = FLUSH_THRESHOLD - 1;
        for (int i = 0; i < items; i++) {
            sstables.add(Entry.add(i, String.valueOf(i)));
        }

        CloseableIterator<Entry<Integer, String>> iterator = sstables.iterator(Direction.FORWARD);
        int expected = 0;
        while (iterator.hasNext()) {
            Entry<Integer, String> entry = iterator.next();
            assertNotNull(entry);
            assertEquals(Integer.valueOf(expected), entry.key);
            assertEquals(String.valueOf(expected), entry.value);
            expected++;
        }
        assertEquals(items, expected);
    }

    @Test
    public void get_returns_items_from_disk() {
        int items = FLUSH_THRESHOLD * 2;
        for (int i = 0; i < items; i++) {
            sstables.add(Entry.add(i, String.valueOf(i)));
        }

        sstables.flushSync();

        for (int i = 0; i < items; i++) {
            Entry<Integer, String> entry = sstables.get(i);
            assertNotNull(entry);
            assertEquals(Integer.valueOf(i), entry.key);
            assertEquals(String.valueOf(i), entry.value);
        }
    }

    @Test
    public void range_forward_scan() {
        int items = FLUSH_THRESHOLD * 5;
        for (int i = 0; i < items; i++) {
            sstables.add(Entry.add(i, String.valueOf(i)));
        }
        CloseableIterator<Entry<Integer, String>> iterator = sstables.iterator(Direction.FORWARD, Range.of(0, items));
        int key = 0;

        while (iterator.hasNext()) {
            Entry<Integer, String> entry = iterator.next();
            assertEquals(Integer.valueOf(key), entry.key);
            key++;
        }
    }

    @Test
    public void iterator_returns_items_from_disk() {
        int items = FLUSH_THRESHOLD * 2;
        for (int i = 0; i < items; i++) {
            sstables.add(Entry.add(i, String.valueOf(i)));
        }

        sstables.flushSync();

        CloseableIterator<Entry<Integer, String>> iterator = sstables.iterator(Direction.FORWARD);
        int expected = 0;
        while (iterator.hasNext()) {
            Entry<Integer, String> entry = iterator.next();
            assertNotNull(entry);
            assertEquals(Integer.valueOf(expected), entry.key);
            assertEquals(String.valueOf(expected), entry.value);
            expected++;
        }
        assertEquals(items, expected);
    }

    @Test
    public void get_returns_items_from_disk_MULTIPLE_SEGMENTS() {
        int items = FLUSH_THRESHOLD * 4;
        for (int i = 0; i < items; i++) {
            sstables.add(Entry.add(i, String.valueOf(i)));
        }

        sstables.flushSync();

        for (int i = 0; i < items; i++) {
            Entry<Integer, String> entry = sstables.get(i);
            assertNotNull(entry);
            assertEquals(Integer.valueOf(i), entry.key);
            assertEquals(String.valueOf(i), entry.value);
        }
    }

    @Test
    public void iterator_returns_items_from_disk_MULTIPLE_SEGMENTS() {
        int diskItems = FLUSH_THRESHOLD * 10;
        for (int i = 0; i < diskItems; i++) {
            sstables.add(Entry.add(i, String.valueOf(i)));
        }

        CloseableIterator<Entry<Integer, String>> iterator = sstables.iterator(Direction.FORWARD);

        long count = Iterators.stream(sstables.iterator(Direction.FORWARD)).count();
        assertEquals(diskItems, count);

        int expected = 0;
        while (iterator.hasNext()) {
            Entry<Integer, String> entry = iterator.next();
            assertNotNull(entry);
            assertEquals(Integer.valueOf(expected), entry.key);
            assertEquals(String.valueOf(expected), entry.value);
            expected++;
        }
    }

    @Test
    public void get_returns_items_from_disk_and_memTable() {
        int items = (int) (FLUSH_THRESHOLD * 6.5);
        for (int i = 0; i < items; i++) {
            sstables.add(Entry.add(i, String.valueOf(i)));
        }

        sstables.flushSync();

        for (int i = 0; i < items; i++) {
            Entry<Integer, String> entry = sstables.get(i);
            assertNotNull(entry);
            assertEquals(Integer.valueOf(i), entry.key);
            assertEquals(String.valueOf(i), entry.value);
        }
    }

    @Test
    public void iterator_returns_items_from_disk_and_memTable() {
        int diskItems = FLUSH_THRESHOLD * 3;
        int memItems = FLUSH_THRESHOLD / 2;
        for (int i = 0; i < diskItems; i++) {
            sstables.add(Entry.add(i, String.valueOf(i)));
        }

        sstables.flushSync();
        for (int i = diskItems; i < diskItems + memItems; i++) {
            sstables.add(Entry.add(i, String.valueOf(i)));
        }

        CloseableIterator<Entry<Integer, String>> iterator = sstables.iterator(Direction.FORWARD);
        int expected = 0;
        while (iterator.hasNext()) {
            Entry<Integer, String> entry = iterator.next();
            assertNotNull(entry);
            assertEquals(Integer.valueOf(expected), entry.key);
            assertEquals(String.valueOf(expected), entry.value);
            expected++;
        }
        assertEquals(diskItems + memItems, expected);
    }

    @Test
    public void iterator_with_range_returns_items_from_disk_and_memTable() {
        int diskItems = FLUSH_THRESHOLD * 3;
        int memItems = FLUSH_THRESHOLD / 2;
        for (int i = 0; i < diskItems; i++) {
            sstables.add(Entry.add(i, String.valueOf(i)));
        }

        sstables.flushSync();
        for (int i = diskItems; i < diskItems + memItems; i++) {
            sstables.add(Entry.add(i, String.valueOf(i)));
        }

        int startKey = 50;
        int endKey = diskItems / 2;

        long count = Iterators.stream(sstables.iterator(Direction.FORWARD, Range.of(startKey, endKey))).count();
        assertEquals(endKey - startKey, count);

        CloseableIterator<Entry<Integer, String>> iterator = sstables.iterator(Direction.FORWARD, Range.of(startKey, endKey));
        int expected = startKey;
        while (iterator.hasNext()) {
            Entry<Integer, String> entry = iterator.next();
            assertNotNull(entry);
            assertEquals(Integer.valueOf(expected), entry.key);
            assertEquals(String.valueOf(expected), entry.value);
            expected++;
        }
    }

    @Test
    public void iterator_with_range_returns_items_from_disk_and_memTable2() {
        int diskItems = (int) (FLUSH_THRESHOLD * 100.5);
        for (int i = 0; i < diskItems; i++) {
            sstables.add(Entry.add(i, String.valueOf(i)));
        }

        int startKey = 50;
        int endKey = diskItems / 2;

        long count = Iterators.stream(sstables.iterator(Direction.FORWARD, Range.of(startKey, endKey))).count();
        assertEquals(endKey - startKey, count);

        CloseableIterator<Entry<Integer, String>> iterator = sstables.iterator(Direction.FORWARD, Range.of(startKey, endKey));
        int expected = startKey;
        while (iterator.hasNext()) {
            Entry<Integer, String> entry = iterator.next();
            assertNotNull(entry);
            assertEquals(Integer.valueOf(expected), entry.key);
            assertEquals(String.valueOf(expected), entry.value);
            expected++;
        }
    }

    @Test
    public void when_memTable_is_FLUSH_THRESHOLD_size_then_its_flushed() {
        for (int i = 0; i < FLUSH_THRESHOLD - 1; i++) {
            CompletableFuture<Void> flush = sstables.add(Entry.add(i, String.valueOf(i)));
            assertNull(flush);
        }

        CompletableFuture<Void> flush = sstables.add(Entry.add(99999999, String.valueOf(1)));
        assertNotNull(flush);
    }

    @Test
    public void flush_future_returns_when_flush_is_completed() throws InterruptedException {
        for (int i = 0; i < FLUSH_THRESHOLD - 1; i++) {
            sstables.add(Entry.add(i, String.valueOf(i)));
        }

        CompletableFuture<Void> flush = sstables.add(Entry.add(99999999, String.valueOf(1)));
        final CountDownLatch latch = new CountDownLatch(1);
        flush.thenRun(latch::countDown);
        if (!latch.await(10, TimeUnit.SECONDS)) {
            fail("Flush callback not called");
        }
    }


    @Test
    public void floor_with_data_on_memTable() {
        TreeSet<Integer> treeMap = new TreeSet<>();
        int items = FLUSH_THRESHOLD - 1;
        for (int i = 0; i < items; i += 5) {
            sstables.add(Entry.add(i, String.valueOf(i)));
            treeMap.add(i);
        }

        for (int i = 0; i < items; i++) {
            Integer expected = treeMap.floor(i);
            Entry<Integer, String> entry = sstables.floor(i);

            assertNotNull("Failed on " + i, entry);
            assertEquals("Failed on " + i, expected, entry.key);
        }
    }

    @Test
    public void floor_with_data_on_disk() {
        TreeSet<Integer> treeMap = new TreeSet<>();
        int items = FLUSH_THRESHOLD * 3;

        for (int i = 0; i < items; i += 5) {
            sstables.add(Entry.add(i, String.valueOf(i)));
            treeMap.add(i);
        }
        sstables.flushSync();

        for (int i = 0; i < items; i++) {
            Integer expected = treeMap.floor(i);
            Entry<Integer, String> entry = sstables.floor(i);

            assertNotNull("Failed on " + i, entry);
            assertEquals("Failed on " + i, expected, entry.key);
        }
    }

    @Test
    public void floor_with_update() {
        TreeSet<Integer> treeMap = new TreeSet<>();
        int items = FLUSH_THRESHOLD * 3;

        for (int i = 0; i < items; i += 5) {
            sstables.add(Entry.add(i, String.valueOf(i)));
            treeMap.add(i);
        }
        sstables.flushSync();

        //update random key
        Random random = new Random(123L);
        for (int i = 0; i < items; i += 5) {
            int val = random.nextInt(items);
            treeMap.add(val);
            sstables.add(Entry.add(val, String.valueOf(val)));
        }
        sstables.flushSync();

        for (int i = 0; i < items; i++) {
            Integer expected = treeMap.floor(i);
            Entry<Integer, String> entry = sstables.floor(i);

            assertNotNull("Failed on " + i, entry);
            assertEquals("Failed on " + i, expected, entry.key);
        }
    }

    @Test
    public void only_last_entry_is_returned_after_multiple_inserts_on_same_key() {
        int items = (int) (FLUSH_THRESHOLD * 3.5);
        int key = 123;
        for (int i = 0; i < items; i++) {
            sstables.add(Entry.add(key, String.valueOf(i)));
        }

        long count = Iterators.stream(sstables.iterator(Direction.FORWARD)).count();
        assertEquals(1, count);

        try (CloseableIterator<Entry<Integer, String>> iterator = sstables.iterator(Direction.FORWARD)) {
            while (iterator.hasNext()) {
                Entry<Integer, String> entry = iterator.next();
                assertEquals(Integer.valueOf(key), entry.key);
                assertEquals(String.valueOf(items - 1), entry.value);
            }
        }

        Entry<Integer, String> found = sstables.get(key);
        assertNotNull(found);
        assertEquals(String.valueOf(items - 1), found.value);
    }

    @Test
    public void deleted_entries_are_not_returned_when_iterating() {
        int items = (int) (FLUSH_THRESHOLD * 3.5);
        for (int i = 0; i < items; i++) {
            sstables.add(Entry.add(i, String.valueOf(i)));
        }
        //remove even keys
        for (int i = 0; i < items; i += 2) {
            sstables.add(Entry.delete(i));
        }

        long count = Iterators.stream(sstables.iterator(Direction.FORWARD)).count();
        assertEquals(items / 2, count);

        try (CloseableIterator<Entry<Integer, String>> iterator = sstables.iterator(Direction.FORWARD)) {
            while (iterator.hasNext()) {
                Entry<Integer, String> entry = iterator.next();
                assertNotEquals(0, entry.key % 2);
            }
        }
    }

    @Test
    public void scan_backwards() {
        int items = (int) (FLUSH_THRESHOLD * 5.5);
        for (int i = 0; i < items; i++) {
            sstables.add(Entry.add(i, String.valueOf(i)));
        }

        long count = Iterators.stream(sstables.iterator(Direction.BACKWARD)).count();
        assertEquals(items, count);

        int expectedKey = items - 1;
        try (CloseableIterator<Entry<Integer, String>> iterator = sstables.iterator(Direction.BACKWARD)) {
            while (iterator.hasNext()) {
                Entry<Integer, String> entry = iterator.next();
                assertEquals(Integer.valueOf(expectedKey), entry.key);
                expectedKey = entry.key - 1;
            }
        }
    }

    @Test
    public void scan_backwards_with_updated_entries() {
        int items = (int) (FLUSH_THRESHOLD * 5.5);
        for (int i = 0; i < items; i++) {
            sstables.add(Entry.add(i, String.valueOf(i)));
        }

        String updatedValue = "EVEN_KEY_UPDATED_VALUE";
        for (int i = 0; i < items; i += 2) {
            sstables.add(Entry.add(i, updatedValue));
        }

        int expectedKey = items - 1;
        try (CloseableIterator<Entry<Integer, String>> iterator = sstables.iterator(Direction.BACKWARD)) {
            while (iterator.hasNext()) {
                Entry<Integer, String> entry = iterator.next();
                assertEquals(Integer.valueOf(expectedKey), entry.key);
                expectedKey = entry.key - 1;
                if (entry.key % 2 == 0) {
                    assertEquals(updatedValue, entry.value);
                }
            }
        }
    }

    @Test
    public void range_scan_backwards() {
        int items = (int) (FLUSH_THRESHOLD * 3.5);
        for (int i = 0; i < items; i++) {
            sstables.add(Entry.add(i, String.valueOf(i)));
        }

        int start = 0;
        int end = items / 2;

        int expectedKey = end - 1;
        int found = 0;
        try (CloseableIterator<Entry<Integer, String>> iterator = sstables.iterator(Direction.BACKWARD, Range.of(start, end))) {
            while (iterator.hasNext()) {
                Entry<Integer, String> entry = iterator.next();
                assertEquals(Integer.valueOf(expectedKey), entry.key);
                expectedKey = entry.key - 1;
                found++;
            }

            assertEquals(end - start, found);
        }
    }


    @Test
    public void range_scan_backwards_with_updated_entries() {
        int items = (int) (FLUSH_THRESHOLD * 5.5);
        for (int i = 0; i < items; i++) {
            sstables.add(Entry.add(i, String.valueOf(i)));
        }

        String updatedValue = "EVEN_KEY_UPDATED_VALUE";
        for (int i = 0; i < items; i += 2) {
            sstables.add(Entry.add(i, updatedValue));
        }

        int start = 0;
        int end = items / 2;

        int expectedKey = end - 1;
        try (CloseableIterator<Entry<Integer, String>> iterator = sstables.iterator(Direction.BACKWARD, Range.of(start, end))) {
            while (iterator.hasNext()) {
                Entry<Integer, String> entry = iterator.next();
                assertEquals(Integer.valueOf(expectedKey), entry.key);
                expectedKey = entry.key - 1;
                if (entry.key % 2 == 0) {
                    assertEquals(updatedValue, entry.value);
                }
            }
        }
    }

}