package io.joshworks.fstore.lsmtree;

import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.util.FileUtils;
import io.joshworks.fstore.lsmtree.sstable.Range;
import io.joshworks.fstore.log.CloseableIterator;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.appender.FlushMode;
import io.joshworks.fstore.lsmtree.sstable.Entry;
import io.joshworks.fstore.serializer.Serializers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class LsmTreeIT {

    private LsmTree<Integer, String> lsmtree;
    private File testDirectory;
    private static final int FLUSH_THRESHOLD = 500000;


    @Before
    public void setUp() {
        testDirectory = FileUtils.testFolder();
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
        FileUtils.tryDelete(testDirectory);
    }

    @Test
    public void insert_scan() {
        int items = FLUSH_THRESHOLD * 15;
        for (int i = 0; i < items; i++) {
            lsmtree.put(i, String.valueOf(i));
        }
        performGets(items);
        performScans(items);
    }


    @Test
    public void scan_duplicate_keys() {
        int items = FLUSH_THRESHOLD * 15;
        for (int i = 0; i < items; i++) {
            lsmtree.put(i, String.valueOf(i));
        }

        Random random = new Random(123L);
        for (int i = 0; i < items / 2; i++) {
            int id = random.nextInt(items);
            lsmtree.put(id, "updated-value");
        }

        performScans(items);
    }

    private void performScans(int items) {
        sequentialKeyScan(Direction.FORWARD, items, lsmtree.iterator(Direction.FORWARD));
        sequentialKeyScan(Direction.BACKWARD, items, lsmtree.iterator(Direction.BACKWARD));

        sequentialKeyScan(Direction.FORWARD, items, lsmtree.iterator(Direction.FORWARD, Range.of(0, items)));
        sequentialKeyScan(Direction.FORWARD, 40, lsmtree.iterator(Direction.FORWARD, Range.of(10, 50)));
        sequentialKeyScan(Direction.FORWARD, 100, lsmtree.iterator(Direction.FORWARD, Range.of(items - 100, items)));

        sequentialKeyScan(Direction.BACKWARD, items, lsmtree.iterator(Direction.BACKWARD, Range.of(0, items)));
        sequentialKeyScan(Direction.BACKWARD, 40, lsmtree.iterator(Direction.BACKWARD, Range.of(10, 50)));
        sequentialKeyScan(Direction.BACKWARD, 100, lsmtree.iterator(Direction.BACKWARD, Range.of(items - 100, items)));
    }

    private void performGets(int items) {
        for (int i = 0; i < items; i++) {
            if(i == 382) {
                System.out.println();
            }
            String val = lsmtree.get(i);
            assertEquals(String.valueOf(i), val);
        }
    }

    private void sequentialKeyScan(Direction direction, int expectedItems, CloseableIterator<Entry<Integer, String>> iterator) {
        try (iterator) {
            long start = System.currentTimeMillis();

            int scanned = 0;
            Integer expectedKey = null;
            while (iterator.hasNext()) {
                Entry<Integer, String> entry = iterator.next();
                scanned++;
                if (expectedKey == null) {
                    expectedKey = Direction.FORWARD.equals(direction) ? entry.key + 1 : entry.key - 1;
                    continue;
                }
                assertEquals(expectedKey, entry.key);
                expectedKey = Direction.FORWARD.equals(direction) ? entry.key + 1 : entry.key - 1;
            }

            assertEquals(expectedItems, scanned);

            System.out.println("SCAN: SCANNED: " + scanned + " in " + (System.currentTimeMillis() - start));
        }
    }
}