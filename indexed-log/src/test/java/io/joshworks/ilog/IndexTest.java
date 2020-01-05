//package io.joshworks.ilog;
//
//import io.joshworks.fstore.core.util.Size;
//import io.joshworks.fstore.core.util.TestUtils;
//import org.junit.After;
//import org.junit.Before;
//import org.junit.Test;
//
//import java.io.File;
//import java.io.IOException;
//import java.util.TreeSet;
//
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertNotNull;
//
//public class IndexTest {
//
//    private Index index;
//    private File testFile = TestUtils.testFile();
//    private static final int ITEMS = 10000;
//
//    @Before
//    public void setUp() {
//        index = new LongIndex(testFile, Size.GB.ofInt(1));
//    }
//
//    @After
//    public void tearDown() throws IOException {
//        index.delete();
//    }
//
//    @Test
//    public void write() {
//
//        int items = 100;
//        TreeSet<Integer> set = new TreeSet<>();
//        for (int i = 0; i < items; i += 5) {
//            index.write(i, i);
//            set.add(i);
//        }
//
//        for (int i = 0; i < items; i++) {
//            long lower = set.floor(i);
//            assertEquals("Failed on " + i, lower, index.floor(i));
//        }
//    }
//
//    @Test
//    public void get() {
//        int items = 100000;
//        for (int i = 0; i < items; i++) {
//            index.write(i, i);
//        }
//
//        for (int i = 0; i < items; i++) {
//            assertEquals("Failed on " + i, i, index.get(i));
//        }
//    }
//
//    @Test
//    public void get_not_existing() {
//        for (int i = 0; i < 100; i++) {
//            index.write(i, i);
//        }
//        assertEquals(Index.NONE, index.get(101));
//    }
//
//    @Test
//    public void lower_step_1() {
//        lowerWithStep(ITEMS, 1);
//    }
//
//    @Test
//    public void lower_step_2() {
//        lowerWithStep(ITEMS, 2);
//    }
//
//    @Test
//    public void lower_step_5() {
//        lowerWithStep(ITEMS, 5);
//    }
//
//    @Test
//    public void lower_step_7() {
//        lowerWithStep(ITEMS, 7);
//    }
//
//    @Test
//    public void higher_step_1() {
//        higherWithStep(ITEMS, 1);
//    }
//
//    @Test
//    public void higher_step_2() {
//        higherWithStep(ITEMS, 2);
//    }
//
//    @Test
//    public void higher_step_5() {
//        higherWithStep(ITEMS, 5);
//    }
//
//    @Test
//    public void higher_step_7() {
//        higherWithStep(ITEMS, 7);
//    }
//
//    @Test
//    public void floor_step_1() {
//        floorWithStep(ITEMS, 1);
//    }
//
//    @Test
//    public void floor_step_2() {
//        floorWithStep(ITEMS, 2);
//    }
//
//    @Test
//    public void floor_step_5() {
//        floorWithStep(ITEMS, 5);
//    }
//
//    @Test
//    public void floor_step_7() {
//        floorWithStep(ITEMS, 7);
//    }
//
//
//    @Test
//    public void ceiling_step_1() {
//        ceilingWithStep(ITEMS, 1);
//    }
//
//    @Test
//    public void ceiling_step_2() {
//        ceilingWithStep(ITEMS, 2);
//    }
//
//    @Test
//    public void ceiling_step_5() {
//        ceilingWithStep(ITEMS, 5);
//    }
//
//    @Test
//    public void ceiling_step_7() {
//        ceilingWithStep(ITEMS, 7);
//    }
//
//    @Test
//    public void floor_with_key_equals_last_entry_returns_last_entry() {
//        addSomeEntries(10);
//
//        //equals last
//        long found = index.floor(index.last());
//
//        assertEquals(index.get(index.last()), found);
//    }
//
//    @Test
//    public void floor_with_key_greater_than_last_entry_returns_last_entry() {
//        addSomeEntries(10);
//
//        //greater than last
//        long found = index.floor(index.last() + 1);
//
//        assertEquals(index.get(index.last()), found);
//    }
//
//    @Test
//    public void floor_with_key_less_than_first_entry_returns_null() {
//        addSomeEntries(10);
//
//        long found = index.floor(index.first() - 1);
//        assertEquals(Index.NONE, found);
//    }
//
//    @Test
//    public void floor_with_key_equals_first_entry_returns_first_entry() {
//        addSomeEntries(10);
//
//        long found = index.floor(index.first());
//
//        assertEquals(index.get(index.first()), found);
//    }
//
//    @Test
//    public void ceiling_with_key_less_than_first_entry_returns_first_entry() {
//        addSomeEntries(10);
//
//        long found = index.ceiling(index.first() - 1);
//
//        assertEquals(index.get(index.first()), found);
//    }
//
//    @Test
//    public void ceiling_with_key_equals_first_entry_returns_first_entry() {
//        addSomeEntries(10);
//
//        long found = index.ceiling(index.first());
//
//        assertEquals(index.get(index.first()), found);
//    }
//
//    @Test
//    public void ceiling_with_key_greater_than_last_entry_returns_null() {
//        addSomeEntries(10);
//
//        long found = index.ceiling(index.last() + 1);
//        assertEquals(Index.NONE, found);
//    }
//
//    @Test
//    public void ceiling_with_key_equals_last_entry_returns_last_entry() {
//        addSomeEntries(10);
//
//        long found = index.ceiling(index.last());
//
//        assertEquals(index.get(index.last()), found);
//    }
//
//    @Test
//    public void higher_with_key_greater_than_lastKey_returns_null() {
//        addSomeEntries(10);
//
//        long found = index.higher(index.last() + 1);
//        assertEquals(Index.NONE, found);
//    }
//
//    @Test
//    public void higher_with_key_equals_lastKey_returns_null() {
//        addSomeEntries(10);
//
//        long found = index.higher(index.last());
//        assertEquals(Index.NONE, found);
//    }
//
//    @Test
//    public void higher_with_key_less_than_firstKey_returns_firstEntry() {
//        addSomeEntries(10);
//
//        long found = index.higher(index.first() - 1);
//
//        assertEquals(index.get(index.first()), found);
//    }
//
//    @Test
//    public void higher_with_key_lowest_key_returns_firstEntry() {
//        addSomeEntries(10);
//
//        long found = index.higher(Integer.MIN_VALUE);
//
//        assertEquals(index.get(index.first()), found);
//    }
//
//    @Test
//    public void lower_with_key_less_than_firstKey_returns_null() {
//        addSomeEntries(10);
//
//        long found = index.lower(index.first() - 1);
//        assertEquals(Index.NONE, found);
//    }
//
//    @Test
//    public void lower_with_key_equals_firstKey_returns_null() {
//        addSomeEntries(10);
//
//        long found = index.lower(index.first());
//        assertEquals(Index.NONE, found);
//    }
//
//    @Test
//    public void lower_with_key_greater_than_lastKey_returns_lastEntry() {
//        addSomeEntries(10);
//
//        long found = index.lower(index.last() + 1);
//
//        assertEquals(index.get(index.last()), found);
//    }
//
//    @Test
//    public void lower_with_key_highest_key_returns_lastEntry() {
//        addSomeEntries(10);
//
//        long found = index.lower(Integer.MAX_VALUE);
//
//        assertEquals(index.get(index.last()), found);
//    }
//
//
//    private void ceilingWithStep(int items, int steps) {
//        TreeSet<Integer> treeSet = new TreeSet<>();
//        for (int i = 0; i < items; i += steps) {
//            treeSet.add(i);
//            index.write(i, i);
//        }
//
//        for (int i = 0; i < items - steps; i += 1) {
//            long expected = treeSet.ceiling(i);
//            long ceiling = index.ceiling(i);
//            assertEquals("Failed on " + i, expected, ceiling);
//        }
//
//        long ceiling = index.ceiling(0);
//        long first = index.first();
//        assertEquals(first, ceiling);
//
//        ceiling = index.ceiling(Integer.MIN_VALUE);
//        first = index.first();
//        assertEquals(first, ceiling);
//    }
//
//    private void lowerWithStep(int items, int steps) {
//        TreeSet<Integer> treeSet = new TreeSet<>();
//        for (int i = 0; i < items; i += steps) {
//            treeSet.add(i);
//            index.write(i, i);
//        }
//
//        for (int i = 1; i < items; i += 1) {
//            long expected = treeSet.lower(i);
//            long lower = index.lower(i);
//            assertNotNull("Failed on " + i, lower);
//            assertEquals("Failed on " + i, expected, lower);
//        }
//
//        long lower = index.lower(0);
//        assertEquals(Index.NONE, lower);
//
//        long lowest = index.lower(Integer.MAX_VALUE);
//        assertEquals(index.get(index.last()), lowest);
//    }
//
//    private void higherWithStep(int items, int steps) {
//        TreeSet<Integer> treeSet = new TreeSet<>();
//        for (int i = 0; i < items; i += steps) {
//            treeSet.add(i);
//            index.write(i, i);
//        }
//
//        for (int i = 0; i < items - steps; i += 1) {
//            long expected = treeSet.higher(i);
//            long higher = index.higher(i);
//            assertNotNull("Failed on " + i, higher);
//            assertEquals("Failed on " + i, expected, higher);
//        }
//
//        long higher = index.higher(index.last());
//        assertEquals(Index.NONE, higher);
//
//        long highest = index.higher(Integer.MAX_VALUE);
//        assertEquals(Index.NONE, highest);
//    }
//
//    private void floorWithStep(int items, int steps) {
//        TreeSet<Integer> treeSet = new TreeSet<>();
//        for (int i = 0; i < items; i += steps) {
//            treeSet.add(i);
//            index.write(i, i);
//        }
//
//        for (int i = 0; i < items; i += 1) {
//            long expected = treeSet.floor(i);
//            long floor = index.floor(i);
//            assertEquals("Failed on " + i, expected, floor);
//        }
//
//        long floor = index.floor(Integer.MAX_VALUE);
//        long last = index.last();
//        assertEquals(last, floor);
//    }
//
//    private void addSomeEntries(int entries) {
//        for (int i = 0; i < entries; i++) {
//            index.write(i, i);
//        }
//    }
//}