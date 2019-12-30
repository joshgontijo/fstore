package io.joshworks.fstore.log.it;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.util.TestUtils;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.appender.LogAppender;
import io.joshworks.fstore.serializer.Serializers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public abstract class PerformanceIT {

    private static final long ENTRIES = 20000000;

    private static final long SEGMENT_SIZE = Size.MB.of(500);
    private static final int MAX_ENTRY_SIZE = Size.MB.ofInt(1);
    private static final int COMPACTION_THRESHOLD = 2;
    private LogAppender<String> appender;

    protected abstract LogAppender<String> appender(File testDirectory);

    private File testDirectory;

    @Before
    public void setUp() {
        testDirectory = TestUtils.testFolder();
        testDirectory.deleteOnExit();
        appender = appender(testDirectory);
    }

    @After
    public void cleanup() {
        IOUtils.closeQuietly(appender);
        TestUtils.deleteRecursively(testDirectory);
    }

    @Test
    public void write_8K_entries() {
        int entrySize = Size.KB.ofInt(8);
        write(entrySize);
        read(Direction.FORWARD, entrySize);
        read(Direction.BACKWARD, entrySize);
    }

    @Test
    public void write_4K_entries() {
        int entrySize = Size.KB.ofInt(4);
        write(entrySize);
        read(Direction.FORWARD, entrySize);
        read(Direction.BACKWARD, entrySize);
    }

    @Test
    public void write_2K_entries() {
        int entrySize = Size.KB.ofInt(2);
        write(entrySize);
        read(Direction.FORWARD, entrySize);
        read(Direction.BACKWARD, entrySize);
    }

    @Test
    public void write_1K_entries() {
        int entrySize = Size.KB.ofInt(1);
        write(entrySize);
        read(Direction.FORWARD, entrySize);
        read(Direction.BACKWARD, entrySize);
    }

    @Test
    public void write_256b_entries() {
        int entrySize = Size.BYTE.ofInt(256);
        write(entrySize);
        read(Direction.FORWARD, entrySize);
        read(Direction.BACKWARD, entrySize);
    }

    @Test
    public void write_96b_entries() {
        int entrySize = Size.BYTE.ofInt(96);
        write(entrySize);
        read(Direction.FORWARD, entrySize);
        read(Direction.BACKWARD, entrySize);
    }

    private void write(int entrySize) {
        byte[] data = new byte[entrySize];
        Arrays.fill(data, (byte) 65);
        String entry = new String(data);
        long start = System.currentTimeMillis();
        for (long i = 0; i < ENTRIES; i++) {
            appender.append(entry);
        }

        appender.flush();

        long total = (System.currentTimeMillis() - start) / 1000;
        System.out.println("WRITTEN " + ENTRIES + " OF SIZE: " + entrySize + " IN: " + total + "s");
        System.out.println("WRITTEN " + appender.physicalSize() + " bytes");
    }

    private void read(Direction direction, int entrySize) {
        LogIterator<String> iterator = appender.iterator(direction);
        long counter = 0;
        long start = System.currentTimeMillis();
        while (iterator.hasNext()) {
            iterator.next();
            counter++;
        }
        long total = (System.currentTimeMillis() - start) / 1000;
        System.out.println("READ " + counter + " OF SIZE: " + entrySize + " IN " + direction + " IN: " + total + "s");

        assertEquals(ENTRIES, counter);
    }


    public static class RafCachedNoCompactionIT extends PerformanceIT {

        @Override
        protected LogAppender<String> appender(File testDirectory) {
            return LogAppender.builder(testDirectory, Serializers.STRING)
                    .segmentSize(SEGMENT_SIZE)
                    .storageMode(StorageMode.RAF_CACHED)
                    .compactionThreshold(COMPACTION_THRESHOLD)
                    .maxEntrySize(MAX_ENTRY_SIZE)
                    .open();
        }
    }

    public static class MMapNoCompactionIT extends PerformanceIT {

        @Override
        protected LogAppender<String> appender(File testDirectory) {
            return LogAppender.builder(testDirectory, Serializers.STRING)
                    .segmentSize(SEGMENT_SIZE)
                    .storageMode(StorageMode.MMAP)
                    .compactionStorageMode(StorageMode.MMAP)
                    .compactionThreshold(COMPACTION_THRESHOLD)
                    .maxEntrySize(MAX_ENTRY_SIZE)
                    .open();
        }
    }

    public static class RafCachedDirectBufferPoolIT extends PerformanceIT {

        @Override
        protected LogAppender<String> appender(File testDirectory) {
            return LogAppender.builder(testDirectory, Serializers.STRING)
                    .segmentSize(SEGMENT_SIZE)
                    .storageMode(StorageMode.RAF_CACHED)
                    .compactionThreshold(COMPACTION_THRESHOLD)
                    .useDirectBufferPool(true)
                    .maxEntrySize(MAX_ENTRY_SIZE)
                    .open();
        }
    }

    public static class MMapDirectBufferPoolIT extends PerformanceIT {

        @Override
        protected LogAppender<String> appender(File testDirectory) {
            return LogAppender.builder(testDirectory, Serializers.STRING)
                    .segmentSize(SEGMENT_SIZE)
                    .storageMode(StorageMode.MMAP)
                    .compactionStorageMode(StorageMode.MMAP)
                    .compactionThreshold(COMPACTION_THRESHOLD)
                    .useDirectBufferPool(true)
                    .maxEntrySize(MAX_ENTRY_SIZE)
                    .open();
        }
    }
}
