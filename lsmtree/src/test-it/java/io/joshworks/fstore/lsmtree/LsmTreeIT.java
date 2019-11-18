package io.joshworks.fstore.lsmtree;

import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.util.FileUtils;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.appender.FlushMode;
import io.joshworks.fstore.serializer.Serializers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Random;

import static io.joshworks.fstore.lsmtree.utils.Utils.assertIterator;

public class LsmTreeIT {

    private LsmTree<Integer, String> lsmtree;
    private File testDirectory;
    private static final int FLUSH_THRESHOLD = 100000;


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
    public void scan_x5() {
        int items = FLUSH_THRESHOLD * 5;
        for (int i = 0; i < items; i++) {
            lsmtree.put(i, String.valueOf(i));
        }

        performScans(items);
    }

    @Test
    public void scan_x50() {
        int items = FLUSH_THRESHOLD * 50;
        for (int i = 0; i < items; i++) {
            lsmtree.put(i, String.valueOf(i));
        }

        performScans(items);
    }

    @Test
    public void scan_duplicate_x50() {
        int items = FLUSH_THRESHOLD * 50;
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
        assertIterator(Direction.FORWARD, items, lsmtree.iterator(Direction.FORWARD));
        assertIterator(Direction.BACKWARD, items, lsmtree.iterator(Direction.BACKWARD));

        assertIterator(Direction.FORWARD, items, lsmtree.iterator(Direction.FORWARD, Range.of(0, items)));
        assertIterator(Direction.FORWARD, 40, lsmtree.iterator(Direction.FORWARD, Range.of(10, 50)));
        assertIterator(Direction.FORWARD, 100, lsmtree.iterator(Direction.FORWARD, Range.of(items - 100, items)));

        assertIterator(Direction.BACKWARD, items, lsmtree.iterator(Direction.BACKWARD, Range.of(0, items)));
        assertIterator(Direction.BACKWARD, 40, lsmtree.iterator(Direction.BACKWARD, Range.of(10, 50)));
        assertIterator(Direction.BACKWARD, 100, lsmtree.iterator(Direction.BACKWARD, Range.of(items - 100, items)));
    }

}