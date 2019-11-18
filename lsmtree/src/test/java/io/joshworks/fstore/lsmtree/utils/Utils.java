package io.joshworks.fstore.lsmtree.utils;

import io.joshworks.fstore.log.CloseableIterator;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.lsmtree.sstable.entry.Entry;

import static org.junit.Assert.assertEquals;

public class Utils {

    public static void assertIterator(Direction direction, int expectedEntryCount, CloseableIterator<Entry<Integer, String>> iterator) {
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
            System.out.println("SCAN: SCANNED: " + scanned + " in " + (System.currentTimeMillis() - start));
            assertEquals(expectedEntryCount, scanned);
        }
    }
}
