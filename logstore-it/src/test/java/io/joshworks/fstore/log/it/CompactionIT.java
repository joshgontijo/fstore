package io.joshworks.fstore.log.it;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.appender.LogAppender;
import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.fstore.testutils.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;


public abstract class CompactionIT {

    public static final long SEGMENT_SIZE = Size.MB.of(128);
    private LogAppender<String> appender;

    protected abstract LogAppender<String> appender(File testDirectory);

    private File testDirectory;

    @Before
    public void setUp() {
        testDirectory = FileUtils.testFolder();
        testDirectory.deleteOnExit();
        appender = appender(testDirectory);
    }

    @After
    public void cleanup() {
        IOUtils.closeQuietly(appender);
        FileUtils.tryDelete(testDirectory);
    }


    @Test
    public void compaction_maintains_record_order() throws InterruptedException {
        File testDir = FileUtils.testFolder();
        LogAppender<String> appender = LogAppender.builder(testDir, Serializers.STRING).segmentSize(Size.MB.of(10)).compactionThreshold(2).open();

        int items = 20000000;
        for (int i = 0; i < items; i++) {
            appender.append(String.valueOf(i));
        }

        System.out.println("Waiting for compaction");
        Thread.sleep(120000); //TODO improve this

        LogIterator<String> iterator = appender.iterator(Direction.FORWARD);

        int idx = 0;
        while (iterator.hasNext()) {
            String found = iterator.next();
            assertEquals(String.valueOf(idx++), found);
        }
    }


    public static class CachedRafLogAppenderIT extends CompactionIT {

        @Override
        protected LogAppender<String> appender(File testDirectory) {
            return LogAppender.builder(testDirectory, Serializers.STRING)
                    .segmentSize(SEGMENT_SIZE)
                    .storageMode(StorageMode.RAF_CACHED)
                    .open();
        }
    }

    public static class MMapLogAppenderIT extends CompactionIT {

        @Override
        protected LogAppender<String> appender(File testDirectory) {
            return LogAppender.builder(testDirectory, Serializers.STRING)
                    .segmentSize(SEGMENT_SIZE)
                    .storageMode(StorageMode.MMAP)
                    .open();
        }
    }

    public static class RafLogAppenderIT extends CompactionIT {

        @Override
        protected LogAppender<String> appender(File testDirectory) {
            return LogAppender.builder(testDirectory, Serializers.STRING)
                    .segmentSize(SEGMENT_SIZE)
                    .storageMode(StorageMode.RAF)
                    .open();
        }
    }


}