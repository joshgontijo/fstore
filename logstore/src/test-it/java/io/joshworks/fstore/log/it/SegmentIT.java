package io.joshworks.fstore.log.it;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.io.StorageProvider;
import io.joshworks.fstore.core.io.buffers.LocalGrowingBufferPool;
import io.joshworks.fstore.core.util.Memory;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.record.DataStream;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.Segment;
import io.joshworks.fstore.log.segment.WriteMode;
import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.fstore.testutils.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;

public abstract class SegmentIT {

    protected static final double CHECKSUM_PROB = 1;
    protected static final int SEGMENT_SIZE = Size.KB.intOf(128);
    protected static final int MAX_ENTRY_SIZE = SEGMENT_SIZE;
    private static final int BUFFER_SIZE = Memory.PAGE_SIZE;

    protected Log<String> segment;
    private File testFile;

    abstract Log<String> open(File file);

    @Before
    public void setUp() {
        testFile = FileUtils.testFile();
        segment = open(testFile);
    }

    @After
    public void cleanup() {
        IOUtils.closeQuietly(segment);
        FileUtils.tryDelete(testFile);
    }

    @Test
    public void segment_position_is_the_same_as_append_position() {
        String data = "hello";

        for (int i = 0; i < 1000; i++) {
            long segPos = segment.position();
            long pos = segment.append(data);
            assertEquals("Failed on " + i, pos, segPos);
        }
    }


    public static class CachedSegmentTest extends SegmentIT {

        @Override
        Log<String> open(File file) {
            return new Segment<>(
                    StorageProvider.of(StorageMode.RAF_CACHED).create(file, SEGMENT_SIZE),
                    Serializers.STRING,
                    new DataStream(new LocalGrowingBufferPool(false), CHECKSUM_PROB, MAX_ENTRY_SIZE, BUFFER_SIZE),
                    "magic",
                    WriteMode.LOG_HEAD);
        }
    }

    public static class MMapSegmentTest extends SegmentIT {

        @Override
        Log<String> open(File file) {
            return new Segment<>(
                    StorageProvider.of(StorageMode.MMAP).create(file, SEGMENT_SIZE),
                    Serializers.STRING,
                    new DataStream(new LocalGrowingBufferPool(false), CHECKSUM_PROB, MAX_ENTRY_SIZE, BUFFER_SIZE), "magic", WriteMode.LOG_HEAD);
        }
    }

    public static class RafSegmentTest extends SegmentIT {

        @Override
        Log<String> open(File file) {
            return new Segment<>(
                    StorageProvider.of(StorageMode.RAF).create(file, SEGMENT_SIZE),
                    Serializers.STRING,
                    new DataStream(new LocalGrowingBufferPool(false), CHECKSUM_PROB, MAX_ENTRY_SIZE, BUFFER_SIZE),
                    "magic",
                    WriteMode.LOG_HEAD);
        }

        @Test(expected = IllegalArgumentException.class)
        public void inserting_record_bigger_than_MAX_RECORD_SIZE_throws_exception() {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < MAX_ENTRY_SIZE + 1; i++) {
                sb.append("a");
            }
            String data = sb.toString();
            segment.append(data);
            segment.flush();
        }
    }

}
