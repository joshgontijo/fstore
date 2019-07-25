package io.joshworks.fstore.lsmtree.sstable;

import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.util.Memory;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.index.cache.NoCache;
import io.joshworks.fstore.log.segment.WriteMode;
import io.joshworks.fstore.log.segment.block.Block;
import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.fstore.testutils.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.TreeSet;

import static io.joshworks.fstore.core.io.Storage.EOF;
import static io.joshworks.fstore.lsmtree.sstable.Entry.NO_MAX_AGE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class SSTableTest {

    private SSTable<Integer, String> sstable;
    private File testFile;


    @Before
    public void setUp() {
        testFile = FileUtils.testFile();
        sstable = open(testFile);
    }

    private SSTable<Integer, String> open(File file) {
        return new SSTable<>(file,
                StorageMode.MMAP,
                Size.MB.of(100),
                Serializers.INTEGER,
                Serializers.VSTRING,
                new BufferPool(Size.MB.ofInt(1)),
                WriteMode.LOG_HEAD,
                Block.vlenBlock(),
                NO_MAX_AGE,
                Codec.noCompression(),
                Codec.noCompression(),
                new NoCache<>(),
                10000,
                0.01,
                Memory.PAGE_SIZE,
                1,
                Memory.PAGE_SIZE);
    }

    @After
    public void tearDown() {
        sstable.close();
        FileUtils.tryDelete(testFile);
    }

    @Test
    public void lower_step_1() {
        lowerWithStep(1000000, 1);
    }

    @Test
    public void lower_step_2() {
        lowerWithStep(1000000, 2);
    }

    @Test
    public void lower_step_5() {
        lowerWithStep(1000000, 5);
    }

    @Test
    public void lower_step_7() {
        lowerWithStep(1000000, 7);
    }

    @Test
    public void higher_step_1() {
        higherWithStep(1000000, 1);
    }

    @Test
    public void higher_step_2() {
        higherWithStep(1000000, 2);
    }

    @Test
    public void higher_step_5() {
        higherWithStep(1000000, 5);
    }

    @Test
    public void higher_step_7() {
        higherWithStep(1000000, 7);
    }

    @Test
    public void floor_step_1() {
        floorWithStep(1000000, 1);
    }

    @Test
    public void floor_step_2() {
        floorWithStep(1000000, 2);
    }

    @Test
    public void floor_step_5() {
        floorWithStep(1000000, 5);
    }

    @Test
    public void floor_step_7() {
        floorWithStep(1000000, 7);
    }


    @Test
    public void ceiling_step_1() {
        ceilingWithStep(1000000, 1);
    }

    @Test
    public void ceiling_step_2() {
        ceilingWithStep(1000000, 2);
    }

    @Test
    public void ceiling_step_5() {
        ceilingWithStep(1000000, 5);
    }

    @Test
    public void ceiling_step_7() {
        ceilingWithStep(1000000, 7);
    }

    @Test
    public void floor_with_key_equals_last_entry_returns_last_entry() {
        addSomeEntries();

        //equals last
        Entry<Integer, String> found = sstable.floor(sstable.lastKey());
        assertNotNull(found);
        assertEquals(sstable.lastKey(), found.key);
    }

    @Test
    public void floor_with_key_greater_than_last_entry_returns_last_entry() {
        addSomeEntries();

        //greater than last
        Entry<Integer, String> found = sstable.floor(sstable.lastKey() + 1);
        assertNotNull(found);
        assertEquals(sstable.lastKey(), found.key);
    }

    @Test
    public void floor_with_key_less_than_first_entry_returns_null() {
        addSomeEntries();

        Entry<Integer, String> found = sstable.floor(sstable.firstKey() - 1);
        assertNull(found);
    }

    @Test
    public void floor_with_key_equals_first_entry_returns_first_entry() {
        addSomeEntries();

        Entry<Integer, String> found = sstable.floor(sstable.firstKey());
        assertNotNull(found);
        assertEquals(sstable.firstKey(), found.key);
    }

    @Test
    public void ceiling_with_key_less_than_first_entry_returns_first_entry() {
        addSomeEntries();

        Entry<Integer, String> found = sstable.ceiling(sstable.firstKey() - 1);
        assertNotNull(found);
        assertEquals(sstable.firstKey(), found.key);
    }

    @Test
    public void ceiling_with_key_equals_first_entry_returns_first_entry() {
        addSomeEntries();

        Entry<Integer, String> found = sstable.ceiling(sstable.firstKey());
        assertNotNull(found);
        assertEquals(sstable.firstKey(), found.key);
    }

    @Test
    public void ceiling_with_key_greater_than_last_entry_returns_null() {
        addSomeEntries();

        Entry<Integer, String> found = sstable.ceiling(sstable.lastKey() + 1);
        assertNull(found);
    }

    @Test
    public void ceiling_with_key_equals_last_entry_returns_last_entry() {
        addSomeEntries();

        Entry<Integer, String> found = sstable.ceiling(sstable.lastKey());
        assertNotNull(found);
        assertEquals(sstable.lastKey(), found.key);
    }

    @Test
    public void higher_with_key_greater_than_lastKey_returns_null() {
        addSomeEntries();

        Entry<Integer, String> found = sstable.higher(sstable.lastKey() + 1);
        assertNull(found);
    }

    @Test
    public void higher_with_key_equals_lastKey_returns_null() {
        addSomeEntries();

        Entry<Integer, String> found = sstable.higher(sstable.lastKey());
        assertNull(found);
    }

    @Test
    public void higher_with_key_less_than_firstKey_returns_firstEntry() {
        addSomeEntries();

        Entry<Integer, String> found = sstable.higher(sstable.firstKey() - 1);
        assertNotNull(found);
        assertEquals(sstable.firstKey(), found.key);
    }

    @Test
    public void higher_with_key_lowest_key_returns_firstEntry() {
        addSomeEntries();

        Entry<Integer, String> found = sstable.higher(Integer.MIN_VALUE);
        assertNotNull(found);
        assertEquals(sstable.firstKey(), found.key);
    }

    @Test
    public void lower_with_key_less_than_firstKey_returns_null() {
        addSomeEntries();

        Entry<Integer, String> found = sstable.lower(sstable.firstKey() - 1);
        assertNull(found);
    }

    @Test
    public void lower_with_key_equals_firstKey_returns_null() {
        addSomeEntries();

        Entry<Integer, String> found = sstable.lower(sstable.firstKey());
        assertNull(found);
    }

    @Test
    public void lower_with_key_greater_than_lastKey_returns_lastEntry() {
        addSomeEntries();

        Entry<Integer, String> found = sstable.lower(sstable.lastKey() + 1);
        assertNotNull(found);
        assertEquals(sstable.lastKey(), found.key);
    }

    @Test
    public void lower_with_key_highest_key_returns_lastEntry() {
        addSomeEntries();

        Entry<Integer, String> found = sstable.lower(Integer.MAX_VALUE);
        assertNotNull(found);
        assertEquals(sstable.lastKey(), found.key);
    }

    @Test
    public void floor_with_deleted_keys_return_correct_entry() {
        for (int i = 0; i < 10000; i++) {
            sstable.append(Entry.add(i, String.valueOf(i)));
        }

        //delete all
        for (int i = 0; i < 10000; i+=2) {
            sstable.append(Entry.add(i, String.valueOf(i)));
        }

        //equals last
        Entry<Integer, String> found = sstable.floor(sstable.lastKey());
        assertNotNull(found);
        assertEquals(sstable.lastKey(), found.key);
    }

    private void addSomeEntries() {
        for (int i = 0; i < 10; i++) {
            sstable.append(Entry.add(i, String.valueOf(i)));
        }
        sstable.roll(1, false);
    }

    private void floorWithStep(int items, int steps) {
        TreeSet<Integer> treeSet = new TreeSet<>();
        for (int i = 0; i < items; i += steps) {
            treeSet.add(i);
            sstable.append(Entry.add(i, String.valueOf(i)));
        }
        sstable.roll(1, false);

        for (int i = 0; i < items; i += 1) {
            if (i == 806707) {
                System.out.println();
            }
            Integer expected = treeSet.floor(i);
            Entry<Integer, String> floor = sstable.floor(i);
            assertNotNull("Failed on " + i, floor);
            assertEquals("Failed on " + i, expected, floor.key);
        }

        Entry<Integer, String> floor = sstable.floor(Integer.MAX_VALUE);
        Entry<Integer, String> last = sstable.last();
        assertEquals(last, floor);
    }

    private void ceilingWithStep(int items, int steps) {
        TreeSet<Integer> treeSet = new TreeSet<>();
        for (int i = 0; i < items; i += steps) {
            if(i == 806707) {
                System.out.println();
            }
            treeSet.add(i);
            long append = sstable.append(Entry.add(i, String.valueOf(i)));
            if(append == EOF) {
                System.out.println();
            }
        }
        sstable.roll(1, false);


        for (int i = 0; i < items - steps; i += 1) {
            if(i == 806707) {
                System.out.println();
            }
            Integer expected = treeSet.ceiling(i);
            Entry<Integer, String> ceiling = sstable.ceiling(i);
            assertNotNull("Failed on " + i, ceiling);
            assertEquals("Failed on " + i, expected, ceiling.key);
        }

        Entry<Integer, String> ceiling = sstable.ceiling(0);
        Entry<Integer, String> first = sstable.first();
        assertEquals(first, ceiling);

        ceiling = sstable.ceiling(Integer.MIN_VALUE);
        first = sstable.first();
        assertEquals(first, ceiling);
    }

    private void lowerWithStep(int items, int steps) {
        TreeSet<Integer> treeSet = new TreeSet<>();
        for (int i = 0; i < items; i += steps) {
            treeSet.add(i);
            sstable.append(Entry.add(i, String.valueOf(i)));
        }
        sstable.roll(1, false);

        for (int i = 1; i < items; i += 1) {
            Integer expected = treeSet.lower(i);
            Entry<Integer, String> lower = sstable.lower(i);
            assertNotNull("Failed on " + i, lower);
            assertEquals("Failed on " + i, expected, lower.key);
        }

        Entry<Integer, String> lower = sstable.lower(0);
        assertNull(lower);

        Entry<Integer, String> lowest = sstable.lower(Integer.MAX_VALUE);
        assertEquals(sstable.last(), lowest);
    }

    private void higherWithStep(int items, int steps) {
        TreeSet<Integer> treeSet = new TreeSet<>();
        for (int i = 0; i < items; i += steps) {
            treeSet.add(i);
            sstable.append(Entry.add(i, String.valueOf(i)));
        }
        sstable.roll(1, false);


        for (int i = 0; i < items - steps; i += 1) {
            Integer expected = treeSet.higher(i);
            Entry<Integer, String> higher = sstable.higher(i);
            assertNotNull("Failed on " + i, higher);
            assertEquals("Failed on " + i, expected, higher.key);
        }

        Entry<Integer, String> higher = sstable.higher(sstable.lastKey());
        assertNull(higher);

        Entry<Integer, String> highest = sstable.higher(Integer.MAX_VALUE);
        assertNull(highest);
    }
}