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
import static org.junit.Assert.assertNull;

public class IndexTest {

    private Index<Integer> index;
    private File testFile = TestUtils.testFile();
    private static final int ITEMS = 10000;

    @Before
    public void setUp() {
        index = new Index<>(testFile, Size.MB.of(10), Integer.BYTES, Serializers.INTEGER);
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

    @Test
    public void get() {
        for (int i = 0; i < 100; i++) {
            index.write(i, i);
        }

        for (int i = 0; i < 100; i++) {
            IndexEntry<Integer> entry = index.get(i);
            assertNotNull(entry);
            assertEquals(Integer.valueOf(i), entry.key);
        }
    }

    @Test
    public void get_not_existing() {
        for (int i = 0; i < 100; i++) {
            index.write(i, i);
        }
        assertNull(index.get(101));
    }

    @Test
    public void lower_step_1() {
        lowerWithStep(ITEMS, 1);
    }

    @Test
    public void lower_step_2() {
        lowerWithStep(ITEMS, 2);
    }

    @Test
    public void lower_step_5() {
        lowerWithStep(ITEMS, 5);
    }

    @Test
    public void lower_step_7() {
        lowerWithStep(ITEMS, 7);
    }

    @Test
    public void higher_step_1() {
        higherWithStep(ITEMS, 1);
    }

    @Test
    public void higher_step_2() {
        higherWithStep(ITEMS, 2);
    }

    @Test
    public void higher_step_5() {
        higherWithStep(ITEMS, 5);
    }

    @Test
    public void higher_step_7() {
        higherWithStep(ITEMS, 7);
    }

    @Test
    public void floor_step_1() {
        floorWithStep(ITEMS, 1);
    }

    @Test
    public void floor_step_2() {
        floorWithStep(ITEMS, 2);
    }

    @Test
    public void floor_step_5() {
        floorWithStep(ITEMS, 5);
    }

    @Test
    public void floor_step_7() {
        floorWithStep(ITEMS, 7);
    }


    @Test
    public void ceiling_step_1() {
        ceilingWithStep(ITEMS, 1);
    }

    @Test
    public void ceiling_step_2() {
        ceilingWithStep(ITEMS, 2);
    }

    @Test
    public void ceiling_step_5() {
        ceilingWithStep(ITEMS, 5);
    }

    @Test
    public void ceiling_step_7() {
        ceilingWithStep(ITEMS, 7);
    }

    @Test
    public void floor_with_key_equals_last_entry_returns_last_entry() {
        addSomeEntries(10);

        //equals last
        IndexEntry<Integer> found = index.floor(index.last.key);
        assertNotNull(found);
        assertEquals(index.last.key, found.key);
    }

    @Test
    public void floor_with_key_greater_than_last_entry_returns_last_entry() {
        addSomeEntries(10);

        //greater than last
        IndexEntry<Integer> found = index.floor(index.last.key + 1);
        assertNotNull(found);
        assertEquals(index.last.key, found.key);
    }

    @Test
    public void floor_with_key_less_than_first_entry_returns_null() {
        addSomeEntries(10);

        IndexEntry<Integer> found = index.floor(index.first.key - 1);
        assertNull(found);
    }

    @Test
    public void floor_with_key_equals_first_entry_returns_first_entry() {
        addSomeEntries(10);

        IndexEntry<Integer> found = index.floor(index.first.key);
        assertNotNull(found);
        assertEquals(index.first.key, found.key);
    }

    @Test
    public void ceiling_with_key_less_than_first_entry_returns_first_entry() {
        addSomeEntries(10);

        IndexEntry<Integer> found = index.ceiling(index.first.key - 1);
        assertNotNull(found);
        assertEquals(index.first.key, found.key);
    }

    @Test
    public void ceiling_with_key_equals_first_entry_returns_first_entry() {
        addSomeEntries(10);

        IndexEntry<Integer> found = index.ceiling(index.first.key);
        assertNotNull(found);
        assertEquals(index.first.key, found.key);
    }

    @Test
    public void ceiling_with_key_greater_than_last_entry_returns_null() {
        addSomeEntries(10);

        IndexEntry<Integer> found = index.ceiling(index.last.key + 1);
        assertNull(found);
    }

    @Test
    public void ceiling_with_key_equals_last_entry_returns_last_entry() {
        addSomeEntries(10);

        IndexEntry<Integer> found = index.ceiling(index.last.key);
        assertNotNull(found);
        assertEquals(index.last.key, found.key);
    }

    @Test
    public void higher_with_key_greater_than_lastKey_returns_null() {
        addSomeEntries(10);

        IndexEntry<Integer> found = index.higher(index.last.key + 1);
        assertNull(found);
    }

    @Test
    public void higher_with_key_equals_lastKey_returns_null() {
        addSomeEntries(10);

        IndexEntry<Integer> found = index.higher(index.last.key);
        assertNull(found);
    }

    @Test
    public void higher_with_key_less_than_firstKey_returns_firstEntry() {
        addSomeEntries(10);

        IndexEntry<Integer> found = index.higher(index.first.key - 1);
        assertNotNull(found);
        assertEquals(index.first.key, found.key);
    }

    @Test
    public void higher_with_key_lowest_key_returns_firstEntry() {
        addSomeEntries(10);

        IndexEntry<Integer> found = index.higher(Integer.MIN_VALUE);
        assertNotNull(found);
        assertEquals(index.first.key, found.key);
    }

    @Test
    public void lower_with_key_less_than_firstKey_returns_null() {
        addSomeEntries(10);

        IndexEntry<Integer> found = index.lower(index.first.key - 1);
        assertNull(found);
    }

    @Test
    public void lower_with_key_equals_firstKey_returns_null() {
        addSomeEntries(10);

        IndexEntry<Integer> found = index.lower(index.first.key);
        assertNull(found);
    }

    @Test
    public void lower_with_key_greater_than_lastKey_returns_lastEntry() {
        addSomeEntries(10);

        IndexEntry<Integer> found = index.lower(index.last.key + 1);
        assertNotNull(found);
        assertEquals(index.last.key, found.key);
    }

    @Test
    public void lower_with_key_highest_key_returns_lastEntry() {
        addSomeEntries(10);

        IndexEntry<Integer> found = index.lower(Integer.MAX_VALUE);
        assertNotNull(found);
        assertEquals(index.last.key, found.key);
    }


    private void ceilingWithStep(int items, int steps) {
        TreeSet<Integer> treeSet = new TreeSet<>();
        for (int i = 0; i < items; i += steps) {
            treeSet.add(i);
            index.write(i, i);
        }

        for (int i = 0; i < items - steps; i += 1) {
            Integer expected = treeSet.ceiling(i);
            IndexEntry<Integer> ceiling = index.ceiling(i);
            assertNotNull("Failed on " + i, ceiling);
            assertEquals("Failed on " + i, expected, ceiling.key);
        }

        IndexEntry<Integer> ceiling = index.ceiling(0);
        IndexEntry<Integer> first = index.first;
        assertEquals(first, ceiling);

        ceiling = index.ceiling(Integer.MIN_VALUE);
        first = index.first;
        assertEquals(first, ceiling);
    }

    private void lowerWithStep(int items, int steps) {
        TreeSet<Integer> treeSet = new TreeSet<>();
        for (int i = 0; i < items; i += steps) {
            treeSet.add(i);
            index.write(i, i);
        }

        for (int i = 1; i < items; i += 1) {
            Integer expected = treeSet.lower(i);
            IndexEntry<Integer> lower = index.lower(i);
            assertNotNull("Failed on " + i, lower);
            assertEquals("Failed on " + i, expected, lower.key);
        }

        IndexEntry<Integer> lower = index.lower(0);
        assertNull(lower);

        IndexEntry<Integer> lowest = index.lower(Integer.MAX_VALUE);
        assertEquals(index.last, lowest);
    }

    private void higherWithStep(int items, int steps) {
        TreeSet<Integer> treeSet = new TreeSet<>();
        for (int i = 0; i < items; i += steps) {
            treeSet.add(i);
            index.write(i, i);
        }

        for (int i = 0; i < items - steps; i += 1) {
            Integer expected = treeSet.higher(i);
            IndexEntry<Integer> higher = index.higher(i);
            assertNotNull("Failed on " + i, higher);
            assertEquals("Failed on " + i, expected, higher.key);
        }

        IndexEntry<Integer> higher = index.higher(index.last.key);
        assertNull(higher);

        IndexEntry<Integer> highest = index.higher(Integer.MAX_VALUE);
        assertNull(highest);
    }

    private void floorWithStep(int items, int steps) {
        TreeSet<Integer> treeSet = new TreeSet<>();
        for (int i = 0; i < items; i += steps) {
            treeSet.add(i);
            index.write(i, i);
        }

        for (int i = 0; i < items; i += 1) {
            Integer expected = treeSet.floor(i);
            IndexEntry<Integer> floor = index.floor(i);
            assertNotNull("Failed on " + i, floor);
            assertEquals("Failed on " + i, expected, floor.key);
        }

        IndexEntry<Integer> floor = index.floor(Integer.MAX_VALUE);
        IndexEntry<Integer> last = index.last;
        assertEquals(last, floor);
    }

    private void addSomeEntries(int entries) {
        for (int i = 0; i < entries; i++) {
            index.write(i, i);
        }
    }
}