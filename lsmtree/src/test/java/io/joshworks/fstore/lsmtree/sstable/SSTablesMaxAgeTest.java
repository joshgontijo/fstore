package io.joshworks.fstore.lsmtree.sstable;

import io.joshworks.fstore.codec.snappy.SnappyCodec;
import io.joshworks.fstore.core.cache.Cache;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.util.FileUtils;
import io.joshworks.fstore.core.util.Memory;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.core.util.Threads;
import io.joshworks.fstore.log.appender.FlushMode;
import io.joshworks.fstore.serializer.Serializers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;

public class SSTablesMaxAgeTest {

    private static final int FLUSH_THRESHOLD = 1000;
    private static final int COMPACTION_THRESHOLD = 3;
    private SSTables<Integer, String> sstables;
    private File testDirectory;
    private final long MAX_AGE_SECONDS = 1;

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
                FLUSH_THRESHOLD,
                Size.KB.ofInt(8),
                StorageMode.MMAP,
                FlushMode.MANUAL,
                new SSTableCompactor<>(MAX_AGE_SECONDS),
                MAX_AGE_SECONDS,
                new SnappyCodec(),
                3,
                false,
                COMPACTION_THRESHOLD,
                false,
                0.01,
                Memory.PAGE_SIZE,
                Cache.softCache());
    }

    @After
    public void tearDown() {
        sstables.close();
        FileUtils.tryDelete(testDirectory);
    }

    @Test
    public void max_aged_ADD_items_are_not_flushed_to_sstable() {
        int items = FLUSH_THRESHOLD - 1;
        for (int i = 0; i < items; i++) {
            sstables.add(Entry.add(i, String.valueOf(i)));
        }

        awaitExpiration();
        sstables.flushSync();
        assertEquals(0, sstables.size());
    }

    @Test
    public void max_aged_deletion_items_are_flushed_to_sstable() {
        sstables.add(Entry.add(1, "A"));
        sstables.add(Entry.delete(1));
        sstables.add(Entry.delete(2));
        sstables.add(Entry.delete(3));

        awaitExpiration();
        sstables.flushSync();
        assertEquals(3, sstables.size());
    }

    private void awaitExpiration() {
        Threads.sleep(MAX_AGE_SECONDS * 1000 + 1000);
    }

}