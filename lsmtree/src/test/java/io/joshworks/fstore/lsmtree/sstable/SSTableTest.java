package io.joshworks.fstore.lsmtree.sstable;

import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.util.Memory;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.index.cache.NoCache;
import io.joshworks.fstore.log.segment.WriteMode;
import io.joshworks.fstore.log.segment.block.VLenBlock;
import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.fstore.testutils.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.TreeSet;

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
                Size.MB.of(20),
                Serializers.INTEGER,
                Serializers.VSTRING,
                new BufferPool(),
                WriteMode.LOG_HEAD,
                VLenBlock.factory(),
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
    public void lower_step_2() {
        lowerWithStep(1000000, 2);
    }

    @Test
    public void lower_step_5() {
        lowerWithStep(1000000, 2);
    }

    @Test
    public void lower_step_7() {
        lowerWithStep(1000000, 2);
    }

    @Test
    public void higher_step_2() {
        higherWithStep(1000000, 2);
    }

    @Test
    public void higher_step_5() {
        higherWithStep(1000000, 2);
    }

    @Test
    public void higher_step_7() {
        higherWithStep(1000000, 2);
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

    private void floorWithStep(int items, int steps) {
        TreeSet<Integer> treeSet = new TreeSet<>();
        for (int i = 0; i < items; i += steps) {
            treeSet.add(i);
            sstable.append(Entry.add(i, String.valueOf(i)));
        }
        sstable.roll(1);

        for (int i = 0; i < items; i += 1) {
            Integer expected = treeSet.floor(i);
            Entry<Integer, String> floor = sstable.floor(i);
            assertNotNull("Failed on " + i, floor);
            assertEquals("Failed on " + i, expected, floor.key);
        }

        Entry<Integer, String> floor = sstable.floor(items + 50);
        Entry<Integer, String> last = sstable.last();
        assertEquals(last, floor);
    }

    private void ceilingWithStep(int items, int steps) {
        TreeSet<Integer> treeSet = new TreeSet<>();
        for (int i = 0; i < items; i += steps) {
            treeSet.add(i);
            sstable.append(Entry.add(i, String.valueOf(i)));
        }
        sstable.roll(1);


        for (int i = 0; i < items - steps; i += 1) {
            Integer expected = treeSet.ceiling(i);
            Entry<Integer, String> ceiling = sstable.ceiling(i);
            assertNotNull("Failed on " + i, ceiling);
            assertEquals("Failed on " + i, Integer.valueOf(expected), ceiling.key);
        }

        Entry<Integer, String> ceiling = sstable.ceiling(0);
        Entry<Integer, String> first = sstable.first();
        assertEquals(first, ceiling);

        ceiling = sstable.ceiling(-50);
        first = sstable.first();
        assertEquals(first, ceiling);
    }

    private void lowerWithStep(int items, int steps) {
        TreeSet<Integer> treeSet = new TreeSet<>();
        for (int i = 0; i < items; i += steps) {
            treeSet.add(i);
            sstable.append(Entry.add(i, String.valueOf(i)));
        }
        sstable.roll(1);

        for (int i = 1; i < items; i += 1) {
            Integer expected = treeSet.lower(i);
            Entry<Integer, String> lower = sstable.lower(i);
            assertNotNull("Failed on " + i, lower);
            assertEquals("Failed on " + i, expected, lower.key);
        }

        Entry<Integer, String> zeroLower = sstable.lower(0);
        assertNull(zeroLower);

        Entry<Integer, String> floor = sstable.lower(items + 50);
        Entry<Integer, String> last = sstable.last();
        assertEquals(last, floor);
    }

    private void higherWithStep(int items, int steps) {
        TreeSet<Integer> treeSet = new TreeSet<>();
        for (int i = 0; i < items; i += steps) {
            treeSet.add(i);
            sstable.append(Entry.add(i, String.valueOf(i)));
        }
        sstable.roll(1);


        for (int i = 0; i < items - steps; i += 1) {
            Integer expected = treeSet.higher(i);
            Entry<Integer, String> higher = sstable.higher(i);
            assertNotNull("Failed on " + i, higher);
            assertEquals("Failed on " + i, expected, higher.key);
        }

        Entry<Integer, String> higher = sstable.higher(sstable.lastKey());
        assertNull(higher);
    }
}