package io.joshworks.ilog;

import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.core.util.TestUtils;
import io.joshworks.fstore.serializer.Serializers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;

public class IndexTest {

    private Index index;
    private File testFile = TestUtils.testFile();
    private static final int ITEMS = 10000;

    @Before
    public void setUp() {
        index = new LongIndex(testFile, Size.GB.ofInt(1));
    }

    @After
    public void tearDown() throws IOException {
        index.delete();
    }


    @Test
    public void get() {
        int items = 100000;
        for (int i = 0; i < items; i++) {
            writeInt(i, i);
        }

        for (int i = 0; i < items; i++) {
            assertEquals("Failed on " + i, i, index.get(bufferOf(i)));
        }
    }

    @Test
    public void get_not_existing() {
        for (int i = 0; i < 100; i++) {
            writeInt(i, i);
        }
        assertEquals(Index.NONE, index.get(bufferOf(101)));
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
        long found = index.floor(bufferOf(10));

        assertEquals(index.get(bufferOf(9)), found);
    }

    @Test
    public void floor_with_key_greater_than_last_entry_returns_last_entry() {
        addSomeEntries(10);

        //greater than last
        long found = index.floor(bufferOf(11));

        assertEquals(index.get(bufferOf(9)), found);
    }

    @Test
    public void floor_with_key_less_than_first_entry_returns_null() {
        addSomeEntries(10);

        long found = index.floor(bufferOf(-1));
        assertEquals(Index.NONE, found);
    }

    @Test
    public void floor_with_key_equals_first_entry_returns_first_entry() {
        addSomeEntries(10);

        long found = index.floor(bufferOf(0));

        assertEquals(index.get(bufferOf(0)), found);
    }

    @Test
    public void ceiling_with_key_less_than_first_entry_returns_first_entry() {
        addSomeEntries(10);

        long found = index.ceiling(bufferOf(-1));

        assertEquals(index.get(bufferOf(0)), found);
    }

    @Test
    public void ceiling_with_key_equals_first_entry_returns_first_entry() {
        addSomeEntries(10);

        long found = index.ceiling(bufferOf(0));

        assertEquals(index.get(bufferOf(0)), found);
    }

    @Test
    public void ceiling_with_key_greater_than_last_entry_returns_null() {
        addSomeEntries(10);

        long found = index.ceiling(bufferOf(11));
        assertEquals(Index.NONE, found);
    }

    @Test
    public void ceiling_with_key_equals_last_entry_returns_last_entry() {
        addSomeEntries(10);

        long found = index.ceiling(bufferOf(10));

        assertEquals(index.get(bufferOf(10)), found);
    }

    @Test
    public void higher_with_key_greater_than_lastKey_returns_null() {
        addSomeEntries(10);

        long found = index.higher(bufferOf(11));
        assertEquals(Index.NONE, found);
    }

    @Test
    public void higher_with_key_equals_lastKey_returns_null() {
        addSomeEntries(10);

        long found = index.higher(bufferOf(10));
        assertEquals(Index.NONE, found);
    }

    @Test
    public void higher_with_key_less_than_firstKey_returns_firstEntry() {
        addSomeEntries(10);

        long found = index.higher(bufferOf(-1));

        assertEquals(index.get(bufferOf(0)), found);
    }

    @Test
    public void higher_with_key_lowest_key_returns_firstEntry() {
        addSomeEntries(10);

        long found = index.higher(bufferOf(Integer.MIN_VALUE));

        assertEquals(index.get(bufferOf(0)), found);
    }

    @Test
    public void lower_with_key_less_than_firstKey_returns_null() {
        addSomeEntries(10);

        long found = index.lower(bufferOf(-1));
        assertEquals(Index.NONE, found);
    }

    @Test
    public void lower_with_key_equals_firstKey_returns_null() {
        addSomeEntries(10);

        long found = index.lower(bufferOf(0));
        assertEquals(Index.NONE, found);
    }

    @Test
    public void lower_with_key_greater_than_lastKey_returns_lastEntry() {
        addSomeEntries(10);

        long found = index.lower(bufferOf(11));

        assertEquals(index.get(bufferOf(9)), found);
    }

    @Test
    public void lower_with_key_highest_key_returns_lastEntry() {
        addSomeEntries(10);

        long found = index.lower(bufferOf(Integer.MAX_VALUE));
        assertEquals(index.get(bufferOf(9)), found);
    }

    private void ceilingWithStep(int items, int steps) {
        TreeSet<Integer> treeSet = new TreeSet<>();
        for (int i = 0; i < items; i += steps) {
            treeSet.add(i);
            writeInt(i, i);
        }

        for (int i = 0; i < items - steps; i += 1) {
            long expected = treeSet.ceiling(i);
            long ceiling = index.ceiling(bufferOf(i));
            assertEquals("Failed on " + i, expected, ceiling);
        }
    }

    private void lowerWithStep(int items, int steps) {
        TreeSet<Integer> treeSet = new TreeSet<>();
        for (int i = 0; i < items; i += steps) {
            treeSet.add(i);
            writeInt(i, i);
        }

        for (int i = 1; i < items; i += 1) {
            long expected = treeSet.lower(i);
            long lower = index.lower(bufferOf(i));
            assertEquals("Failed on " + i, expected, lower);
        }

    }

    private void higherWithStep(int items, int steps) {
        TreeSet<Integer> treeSet = new TreeSet<>();
        for (int i = 0; i < items; i += steps) {
            treeSet.add(i);
            writeInt(i, i);
        }

        for (int i = 0; i < items - steps; i += 1) {
            long expected = treeSet.higher(i);
            long higher = index.higher(bufferOf(i));
            assertEquals("Failed on " + i, expected, higher);
        }
    }

    private void floorWithStep(int items, int steps) {
        TreeSet<Integer> treeSet = new TreeSet<>();
        for (int i = 0; i < items; i += steps) {
            treeSet.add(i);
            writeInt(i, i);
        }

        for (int i = 0; i < items; i += 1) {
            long expected = treeSet.floor(i);
            long floor = index.floor(bufferOf(i));
            assertEquals("Failed on " + i, expected, floor);
        }
    }

    private void addSomeEntries(int entries) {
        for (int i = 0; i < entries; i++) {
            writeInt(i, i);
        }
    }

    private static ByteBuffer bufferOf(long val) {
        return ByteBuffer.allocate(Long.BYTES).putLong(val).flip();
    }

    private void writeInt(long key, long pos) {
        var record = Record.create(key, Serializers.LONG, "value-" + key, Serializers.VSTRING, ByteBuffer.allocate(1024));
        index.write(record, pos);
    }
    
}