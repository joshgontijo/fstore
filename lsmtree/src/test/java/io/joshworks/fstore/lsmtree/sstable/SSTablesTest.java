package io.joshworks.fstore.lsmtree.sstable;

import io.joshworks.fstore.codec.snappy.SnappyCodec;
import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.util.FileUtils;
import io.joshworks.fstore.core.util.Memory;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.core.util.Threads;
import io.joshworks.fstore.log.appender.FlushMode;
import io.joshworks.fstore.log.segment.block.Block;
import io.joshworks.fstore.serializer.Serializers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Random;
import java.util.TreeSet;

import static io.joshworks.fstore.lsmtree.sstable.Entry.NO_MAX_AGE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class SSTablesTest {

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
                StorageMode.MMAP,
                FlushMode.MANUAL,
                Block.vlenBlock(),
                new SSTableCompactor<>(NO_MAX_AGE),
                NO_MAX_AGE,
                new SnappyCodec(),
                Codec.noCompression(),
                1000000,
                0.01,
                Memory.PAGE_SIZE,
                50,
                12000);
    }

    @After
    public void tearDown() {
        sstables.close();
        FileUtils.tryDelete(testDirectory);
    }

    @Test
    public void writeTo_returns_the_actual_written_items() {
        int items = 1000;
        MemTable<Integer, String> memTable = new MemTable<>();
        for (int i = 0; i < items; i++) {
            memTable.add(Entry.add(i, String.valueOf(i)));
        }

        long written = memTable.writeTo(sstables, NO_MAX_AGE);
        assertEquals(items, written);
    }

    @Test
    public void max_aged_items_are_not_flushed_to_sstable() {
        int items = 1000;
        MemTable<Integer, String> memTable = new MemTable<>();
        for (int i = 0; i < items; i++) {
            memTable.add(Entry.add(i, String.valueOf(i)));
        }

        Threads.sleep(2000);
        long written = memTable.writeTo(sstables, 1);
        assertEquals(0, written);
    }

    @Test
    public void floor() {
        TreeSet<Integer> treeMap = new TreeSet<>();
        int items = 10000;
        int itemsPerSegment = 10000;
        MemTable<Integer, String> memTable = new MemTable<>();

        int x = 0;
        for (int i = 0; i < items; i += 5) {
            memTable.add(Entry.add(i, String.valueOf(i)));
            treeMap.add(i);

            if (++x % itemsPerSegment == 0) {
                memTable.writeTo(sstables, NO_MAX_AGE);
                memTable = new MemTable<>();
            }
        }
        memTable.writeTo(sstables, NO_MAX_AGE);

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
        int segments = 2;
        int itemsPerSegment = 1000;
        MemTable<Integer, String> memTable = new MemTable<>();

        for (int segment = 0; segment < segments; segment++) {
            for (int i = 0; i < itemsPerSegment; i += 5) {
                memTable.add(Entry.add(i, String.valueOf(i)));
                treeMap.add(i);
            }
            memTable.writeTo(sstables, NO_MAX_AGE);
            memTable = new MemTable<>();
        }

        Random random = new Random(123L);
        for (int segment = 0; segment < segments; segment++) {
            for (int i = 0; i < itemsPerSegment; i += 5) {
                int val = random.nextInt(segments - 1);
                treeMap.add(val);
                memTable.add(Entry.add(val, String.valueOf(val)));
            }
            memTable.writeTo(sstables, NO_MAX_AGE);
            memTable = new MemTable<>();
        }
        memTable.writeTo(sstables, NO_MAX_AGE);

        for (int i = 0; i < itemsPerSegment; i++) {
            Integer expected = treeMap.floor(i);
            Entry<Integer, String> entry = sstables.floor(i);

            assertNotNull("Failed on " + i, entry);
            assertEquals("Failed on " + i, expected, entry.key);
        }
    }
}