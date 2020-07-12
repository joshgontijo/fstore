package io.joshworks.ilog;

import io.joshworks.fstore.core.util.TestUtils;
import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.ilog.index.Index;
import io.joshworks.ilog.index.IndexFunction;
import io.joshworks.ilog.index.RowKey;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.TreeSet;

import static io.joshworks.fstore.core.io.buffers.Buffers.wrap;
import static org.junit.Assert.assertEquals;

public class IndexTest {

    private Index index;
    private File testFile = TestUtils.testFile();
    private static final int ITEMS = 100000;

    @Before
    public void setUp() {
        index = new Index(testFile, ITEMS, RowKey.LONG);
    }

    @After
    public void tearDown() {
        index.delete();
    }

    private int get(long key) {
        return index.find(wrap(key), IndexFunction.EQUALS);
    }

    private int lower(long key) {
        return index.find(wrap(key), IndexFunction.LOWER);
    }

    private int higher(long key) {
        return index.find(wrap(key), IndexFunction.HIGHER);
    }

    private int ceiling(long key) {
        return index.find(wrap(key), IndexFunction.CEILING);
    }

    private int floor(long key) {
        return index.find(wrap(key), IndexFunction.FLOOR);
    }

    @Test
    public void get() {
        for (int i = 0; i < ITEMS; i++) {
            addToIndex(i);
        }

        for (int i = 0; i < ITEMS; i++) {
            assertIndexPosition("Failed on " + i, i, get(i));
        }
    }

    @Test
    public void read_entry_size() {
        long key = 111;
        var record = RecordUtils.create(key, Serializers.LONG, "value-" + 123, Serializers.VSTRING);
        int expectedSize = record.recordSize();
        index.write(record, 123);

        int idx = get(key);
        assertEquals(0, idx);
        assertEquals(expectedSize, index.readEntrySize(idx));

    }

    @Test
    public void get_not_existing() {
        for (int i = 0; i < 100; i++) {
            addToIndex(i);
        }
        assertIndexPosition(Index.NONE, get(101));
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
        int found = floor(10);

        assertIndexPosition(get(9), found);
    }

    @Test
    public void floor_with_key_greater_than_last_entry_returns_last_entry() {
        addSomeEntries(10);

        //greater than last
        int found = floor(11);

        assertIndexPosition(get(9), found);
    }

    @Test
    public void floor_with_key_less_than_first_entry_returns_null() {
        addSomeEntries(10);

        int found = floor(-1);
        assertIndexPosition(Index.NONE, found);
    }

    @Test
    public void floor_with_key_equals_first_entry_returns_first_entry() {
        addSomeEntries(10);

        int found = floor(0);

        assertIndexPosition(get(0), found);
    }

    @Test
    public void ceiling_with_key_less_than_first_entry_returns_first_entry() {
        addSomeEntries(10);

        int found = ceiling(-1);

        assertIndexPosition(get(0), found);
    }

    @Test
    public void ceiling_with_key_equals_first_entry_returns_first_entry() {
        addSomeEntries(10);

        int found = ceiling(0);

        assertIndexPosition(get(0), found);
    }

    @Test
    public void ceiling_with_key_greater_than_last_entry_returns_null() {
        addSomeEntries(10);

        int found = ceiling(11);
        assertIndexPosition(Index.NONE, found);
    }

    @Test
    public void ceiling_with_key_equals_last_entry_returns_last_entry() {
        addSomeEntries(10);

        int found = ceiling(10);

        assertIndexPosition(get(10), found);
    }

    @Test
    public void higher_with_key_greater_than_lastKey_returns_null() {
        addSomeEntries(10);

        int found = higher(11);
        assertIndexPosition(Index.NONE, found);
    }

    @Test
    public void higher_with_key_equals_lastKey_returns_null() {
        addSomeEntries(10);

        int found = higher(10);
        assertIndexPosition(Index.NONE, found);
    }

    @Test
    public void higher_with_key_less_than_firstKey_returns_firstEntry() {
        addSomeEntries(10);

        int found = higher(-1);

        assertIndexPosition(get(0), found);
    }

    @Test
    public void higher_with_key_lowest_key_returns_firstEntry() {
        addSomeEntries(10);

        int found = higher(Integer.MIN_VALUE);

        assertIndexPosition(get(0), found);
    }

    @Test
    public void lower_with_key_less_than_firstKey_returns_null() {
        addSomeEntries(10);

        int found = lower(-1);
        assertIndexPosition(Index.NONE, found);
    }

    @Test
    public void lower_with_key_equals_firstKey_returns_null() {
        addSomeEntries(10);

        int found = lower(0);
        assertIndexPosition(Index.NONE, found);
    }

    @Test
    public void lower_with_key_greater_than_lastKey_returns_lastEntry() {
        addSomeEntries(10);

        int found = lower(11);

        assertIndexPosition(get(9), found);
    }

    @Test
    public void lower_with_key_highest_key_returns_lastEntry() {
        addSomeEntries(10);

        int found = lower(Integer.MAX_VALUE);
        assertIndexPosition(get(9), found);
    }

    private void assertIndexPosition(String message, long expected, int idx) {
        long val = index.readPosition(idx);
        assertEquals(message, expected, val);
    }

    private void assertIndexPosition(long expected, int idx) {
        long val = index.readPosition(idx);
        assertEquals(expected, val);
    }

    private void ceilingWithStep(int items, int steps) {
        TreeSet<Integer> treeSet = new TreeSet<>();
        for (int i = 0; i < items; i += steps) {
            treeSet.add(i);
            addToIndex(i);
        }

        for (int i = 0; i < items - steps; i += 1) {
            long expected = treeSet.ceiling(i);
            int ceiling = ceiling(i);
            assertIndexPosition("Failed on " + i, expected, ceiling);
        }
    }

    private void lowerWithStep(int items, int steps) {
        TreeSet<Integer> treeSet = new TreeSet<>();
        for (int i = 0; i < items; i += steps) {
            treeSet.add(i);
            addToIndex(i);
        }

        for (int i = 1; i < items; i += 1) {
            long expected = treeSet.lower(i);
            int lower = lower(i);
            assertIndexPosition("Failed on " + i, expected, lower);
        }

    }

    private void higherWithStep(int items, int steps) {
        TreeSet<Integer> treeSet = new TreeSet<>();
        for (int i = 0; i < items; i += steps) {
            treeSet.add(i);
            addToIndex(i);
        }

        for (int i = 0; i < items - steps; i += 1) {
            long expected = treeSet.higher(i);
            int higher = higher(i);
            assertIndexPosition("Failed on " + i, expected, higher);
        }
    }

    private void floorWithStep(int items, int steps) {
        TreeSet<Integer> treeSet = new TreeSet<>();
        for (int i = 0; i < items; i += steps) {
            treeSet.add(i);
            addToIndex(i);
        }

        for (int i = 0; i < items; i += 1) {
            long expected = treeSet.floor(i);
            int floor = floor(i);
            assertIndexPosition("Failed on " + i, expected, floor);
        }
    }

    private void addSomeEntries(int entries) {
        for (int i = 0; i < entries; i++) {
            addToIndex(i);
        }
    }

    private void addToIndex(long key) {
        var record = RecordUtils.create(key, Serializers.LONG, "value-" + key, Serializers.VSTRING);
        index.write(record, key); //using key as position
    }

}