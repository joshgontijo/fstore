package io.joshworks.fstore.log.segment;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.io.StorageProvider;
import io.joshworks.fstore.core.io.buffers.LocalGrowingBufferPool;
import io.joshworks.fstore.core.util.Memory;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.SegmentIterator;
import io.joshworks.fstore.log.record.DataStream;
import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.fstore.testutils.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public abstract class SegmentReaderTest {

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
    public void endOfLog_always_return_false_for_LOG_HEAD_segment() {
        writeFully(segment);

        SegmentIterator<String> iterator = segment.iterator(Direction.FORWARD);
        while (iterator.hasNext()) {
            assertFalse(iterator.endOfLog());
            iterator.next();
        }
        assertFalse(iterator.endOfLog());
    }

    @Test
    public void endOfLog_is_true_only_when_segment_is_not_LOG_HEAD_and_iterator_has_read_all_entries() {
        writeFully(segment);
        segment.roll(1);

        SegmentIterator<String> iterator = segment.iterator(Direction.FORWARD);
        while (iterator.hasNext()) {
            assertFalse(iterator.endOfLog());
            iterator.next();
        }

        assertTrue(iterator.endOfLog());
    }

    @Test
    public void null_is_returned_when_no_data_is_available() {
        segment.append("a");

        SegmentIterator<String> iterator = segment.iterator(Direction.FORWARD);
        assertNotNull(iterator.next());
        assertNull(iterator.next());
    }


    private List<Long> writeFully(Log<String> segment) {
        List<Long> positions = new ArrayList<>();
        long pos;
        int i = 0;
        while ((pos = segment.append(String.valueOf(i++))) > 0) {
            positions.add(pos);
        }
        return positions;
    }

    public static class CachedSegmentTest extends SegmentReaderTest {

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

    public static class MMapSegmentTest extends SegmentReaderTest {

        @Override
        Log<String> open(File file) {
            return new Segment<>(
                    StorageProvider.of(StorageMode.MMAP).create(file, SEGMENT_SIZE),
                    Serializers.STRING,
                    new DataStream(new LocalGrowingBufferPool(false), CHECKSUM_PROB, MAX_ENTRY_SIZE, BUFFER_SIZE), "magic", WriteMode.LOG_HEAD);
        }
    }

    public static class RafSegmentTest extends SegmentReaderTest {

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