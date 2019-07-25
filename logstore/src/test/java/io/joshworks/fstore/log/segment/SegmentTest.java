package io.joshworks.fstore.log.segment;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.util.Memory;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.SegmentIterator;
import io.joshworks.fstore.log.iterators.Iterators;
import io.joshworks.fstore.log.record.RecordHeader;
import io.joshworks.fstore.log.segment.header.LogHeader;
import io.joshworks.fstore.log.segment.header.Type;
import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.fstore.core.util.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class SegmentTest {

    protected static final double CHECKSUM_PROB = 1;
    protected static final int SEGMENT_SIZE = Size.KB.ofInt(128);
    private static final int READ_PAGE_SIZE = Memory.PAGE_SIZE;
    private static final int MAX_ENTRY_SIZE = Size.MB.ofInt(1);

    protected Segment<String> segment;
    private File testFile;

    abstract Segment<String> open(File file);

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

    @Test
    public void writePosition_reopen() {
        String data = "hello";
        segment.append(data);
        segment.flush();

        long position = segment.position();
        segment.close();

        segment = open(testFile);

        assertEquals(position, segment.position());
    }

    @Test
    public void write() {
        String data = "hello";
        segment.append(data);
        segment.flush();

        LogIterator<String> logIterator = segment.iterator(Direction.FORWARD);
        assertTrue(logIterator.hasNext());
        assertEquals(data, logIterator.next());
    }

    @Test
    public void EOF_is_returned_when_written_more_than_dataSize() {
        String data = "a";
        do {
            long pos = segment.append(data);
            assertTrue(pos > 0);
        } while (segment.remaining() > 0);

        long pos = segment.append(data);
        assertEquals(Storage.EOF, pos);
    }

    @Test
    public void logical_size_is_correct_after_rolling() {
        segment.append("a");
        segment.roll(1, false);
        long logicalSize = segment.logicalSize();

        segment.close();
        segment = open(testFile);
        assertEquals(logicalSize, segment.logicalSize());
    }

    @Test
    public void reader_reopen() {
        String data = "hello";
        segment.append(data);
        segment.flush();

        LogIterator<String> logIterator = segment.iterator(Direction.FORWARD);
        assertTrue(logIterator.hasNext());
        assertEquals(data, logIterator.next());

        long position = segment.position();
        segment.close();

        segment = open(testFile);

        logIterator = segment.iterator(Direction.FORWARD);
        assertEquals(position, segment.position());
        assertTrue(logIterator.hasNext());
        assertEquals(data, logIterator.next());
    }

    @Test
    public void multiple_readers() {
        String data = "hello";
        segment.append(data);
        segment.flush();

        LogIterator<String> logIterator1 = segment.iterator(Direction.FORWARD);
        assertTrue(logIterator1.hasNext());
        assertEquals(data, logIterator1.next());

        LogIterator<String> logIterator2 = segment.iterator(Direction.FORWARD);
        assertTrue(logIterator2.hasNext());
        assertEquals(data, logIterator2.next());
    }

    @Test
    public void big_entry() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < segment.dataSize() - RecordHeader.HEADER_OVERHEAD; i++) {
            sb.append("a");
        }
        String data = sb.toString();
        long pos = segment.append(data);
        assertEquals(Log.START, pos);

        LogIterator<String> logIterator1 = segment.iterator(Direction.FORWARD);
        assertTrue(logIterator1.hasNext());
        assertEquals(data, logIterator1.next());
    }

    @Test
    public void get() {
        List<Long> positions = new ArrayList<>();

        int items = 10;
        for (int i = 0; i < items; i++) {
            long pos = segment.append(String.valueOf(i));
            assertTrue("Unexpected end of segment", pos > 0);
            positions.add(pos);
        }
        segment.flush();

        for (int i = 0; i < items; i++) {
            String found = segment.get(positions.get(i));
            assertEquals(String.valueOf(i), found);
        }
    }

    @Test
    public void header_is_stored() throws IOException {
        File file = FileUtils.testFile();
        Log<String> testSegment = null;
        try {

            testSegment = open(file);
            assertTrue(testSegment.created() > 0);
            assertEquals(0, testSegment.entries());
            assertEquals(0, testSegment.level());
            assertFalse(testSegment.readOnly());

            testSegment.close();

            testSegment = open(file);
            assertTrue(testSegment.created() > 0);
            assertEquals(0, testSegment.entries());
            assertEquals(0, testSegment.level());
            assertFalse(testSegment.readOnly());

            testSegment.append("a");
            testSegment.roll(1, false);

            testSegment.close();

            assertEquals(1, testSegment.entries());
            assertEquals(1, testSegment.level());
            assertTrue(testSegment.readOnly());

        } finally {
            if (testSegment != null) {
                testSegment.close();
            }
            FileUtils.tryDelete(file);
        }
    }

    @Test
    public void entries_are_not_incremented_when_segment_is_full() {
        writeFully(segment);
        long entries = segment.entries();
        segment.append("a");
        assertEquals(entries, segment.entries());
    }

    @Test
    public void position_is_not_incremented_when_segment_is_full() {
        writeFully(segment);
        long position = segment.position();
        segment.append("a");
        assertEquals(position, segment.position());
    }

    @Test
    public void EOF_is_returned_when_no_space_is_available() {
        writeFully(segment);
        long pos = segment.append("a");
        assertEquals(Storage.EOF, pos);
    }

    @Test
    public void iterator_return_all_entries() {
        List<Long> positions = writeFully(segment);
        long entries = Iterators.stream(segment.iterator(Direction.FORWARD)).count();
        assertEquals(entries, positions.size());
    }

    @Test
    public void entries_matches_actual() {
        List<Long> positions = writeFully(segment);
        segment.flush();
        assertEquals(segment.entries(), positions.size());
    }

    @Test
    public void segment_is_only_deleted_when_no_readers_are_active() {
        File file = FileUtils.testFile();
        try (Log<String> testSegment = open(file)) {

            testSegment.append("a");

            LogIterator<String> reader = testSegment.iterator(Direction.FORWARD);

            testSegment.delete();
            assertTrue(Files.exists(file.toPath()));

            reader.close();
            testSegment.delete();
            assertFalse(Files.exists(file.toPath()));

        } catch (Exception e) {
            FileUtils.tryDelete(file);
        }
    }

    @Test
    public void segment_read_backwards() throws IOException {
        int entries = writeFully(segment).size();
        segment.flush();

        int current = entries - 1;
        try (LogIterator<String> iterator = segment.iterator(Direction.BACKWARD)) {
            while (iterator.hasNext()) {
                String next = iterator.next();
                assertEquals(String.valueOf(current--), next);
            }
        }
        assertEquals(-1, current);
    }

    @Test
    public void iterator_continuously_read_from_disk() {
        LogIterator<String> iterator = segment.iterator(Direction.FORWARD);
        assertFalse(iterator.hasNext());

        segment.append("a");

        assertTrue(iterator.hasNext());
        assertEquals("a", iterator.next());

    }

    @Test
    public void reader_forward_maintain_correct_position() throws IOException {
        List<Long> positions = writeFully(segment);
        segment.flush();

        try (LogIterator<String> iterator = segment.iterator(Direction.FORWARD)) {
            int idx = 0;
            while (iterator.hasNext()) {
                assertEquals("Failed at " + idx, positions.get(idx++), Long.valueOf(iterator.position()));
                iterator.next();
            }
        }
    }

    @Test
    public void reader_backward_maintain_correct_position() throws IOException {
        List<Long> positions = writeFully(segment);
        segment.flush();

        Collections.reverse(positions);
        try (LogIterator<String> iterator = segment.iterator(Direction.BACKWARD)) {
            int idx = 0;
            while (iterator.hasNext()) {
                iterator.next();
                assertEquals("Failed on " + idx, positions.get(idx++), Long.valueOf(iterator.position()));
            }
        }
    }

    @Test
    public void segment_read_backwards_returns_false_when_empty_log() throws IOException {
        try (LogIterator<String> iterator = segment.iterator(Direction.BACKWARD)) {
            assertFalse(iterator.hasNext());
        }
    }

    @Test
    public void actualDataSize_returns_actual_writen_bytes_after_segment_is_reopened() {
        var data = "AAAAA";
        segment.append(data);
        segment.roll(1, false);

        long logSize = segment.actualDataSize();

        assertEquals(data.length() + RecordHeader.HEADER_OVERHEAD, logSize);
    }

    @Test
    public void actualDataSize_returns_actual_written_bytes_in_the_data_area() {
        var data = "AAAAA";
        segment.append(data);
        segment.roll(1, false);

        segment.close();
        segment = open(testFile);

        long logSize = segment.actualDataSize();

        assertEquals(data.length() + RecordHeader.HEADER_OVERHEAD, logSize);
    }

    @Test
    public void remaining_returns_logSize_when_no_data_has_been_written() {
        long logSize = segment.dataSize();
        long remaining = segment.remaining();
        assertEquals(logSize, remaining);
    }

    @Test
    public void remaining_returns_correct_data_after_writing_to_log() {
        var data = "AAAAA";
        segment.append(data);
        long remaining = segment.remaining();
        assertEquals(segment.dataSize() - data.length() - RecordHeader.HEADER_OVERHEAD, remaining);
    }

    @Test
    public void logSize_is_kept_original_when_reopening_empty_segment() {

        long logSize = segment.dataSize();
        segment.close();
        segment = open(testFile);
        long logSizeAfterReopening = segment.dataSize();

        assertEquals(logSize, logSizeAfterReopening);
    }

    @Test
    public void fileSize_returns_the_physical_file_size() {
        long logSize = segment.physicalSize();
        long physicalSize = testFile.length();
        assertEquals(logSize, physicalSize);
    }

    @Test
    public void trimming_a_segment_shrinks_the_physical_file() {
        long logSize = segment.physicalSize();
        segment.append("a");
        segment.roll(1, true);

        long afterTruncating = segment.physicalSize();
        assertTrue(afterTruncating < logSize);
    }

    @Test
    public void readOnly_returns_true_when_segment_is_rolled() {
        var data = "AAAAA";
        segment.append(data);
        segment.roll(1, false);

        assertTrue(segment.readOnly());
    }

    @Test
    public void readOnly_returns_true_when_segment_is_marked_for_deleted() {
        var data = "AAAAA";
        segment.append(data);
        segment.roll(1, false);
        segment.delete();

        assertTrue(segment.readOnly());
    }

    @Test
    public void readOnly_returns_true_when_MERGE_OUT_segment_is_rolled() throws IOException {
        segment.close();
        segment = mergeOutSegment();

        segment.roll(1, false);
        assertTrue(segment.readOnly());
    }

    @Test
    public void readOnly_returns_false_when_MERGE_OUT_segment_is_reopened() throws IOException {
        segment.close();
        segment = mergeOutSegment();

        segment.close();
        segment = mergeOutSegment();
        assertFalse(segment.readOnly());
    }

    @Test
    public void position_is_max_writePosition_after_rolling_segment() {
        segment.append("a");
        segment.append("b");
        long lastPos = segment.position();
        segment.roll(1, false);

        long foundPos = segment.position();

        assertEquals(lastPos, foundPos);
    }

    @Test
    public void position_of_rolled_segment_is_set_to_actualDataSize_when_reopened() {
        segment.append("a");
        segment.append("b");
        segment.position();
        segment.roll(1, false);

        segment.close();
        segment = open(testFile);

        long foundPos = segment.position();
        long actualDataSize = segment.actualDataSize();
        assertEquals(Log.START + actualDataSize, foundPos);
    }

    @Test
    public void segment_is_not_deleted_until_all_readers_are_released_but_is_marked_to_delete() throws IOException {
        SegmentIterator<String> iterator = segment.iterator(Direction.FORWARD);
        segment.delete();

        Type type = segment.type();
        assertEquals(Type.DELETED, type);


        iterator.close();
    }

    @Test
    public void closing_iterator_releases_the_segment_resource() throws IOException {
        try (SegmentIterator<String> iterator = segment.iterator(Direction.FORWARD)) {
            assertFalse(iterator.hasNext()); //not really useful
        }

        segment.close();
        try {
            segment.append("a");
        } catch (SegmentException e) {
            return;
        }
        fail("Expected segment to be closed");
    }

    @Test(expected = SegmentException.class)
    public void cannot_acquire_iterator_of_a_deleted_segment() throws IOException {
        try (SegmentIterator<String> iterator = segment.iterator(Direction.FORWARD)) {
            segment.delete();
            segment.iterator(Direction.FORWARD);
        }
    }

    @Test(expected = SegmentException.class)
    public void cannot_acquire_iterator_of_a_closed_segment() throws IOException {
        segment.close();
        segment.iterator(Direction.FORWARD);
    }

    @Test(expected = SegmentException.class)
    public void cannot_acquire_iterator_of_a_segment_marked_for_deletion() {
        SegmentIterator<String> iterator = segment.iterator(Direction.FORWARD);
        segment.delete();

        SegmentIterator<String> anotherIterator = segment.iterator(Direction.FORWARD);
    }

    @Test
    public void header_is_the_same_after_reopening() {
        LogHeader original = segment.header;
        segment.close();
        segment = open(testFile);
        LogHeader found = segment.header;
        assertEquals(original, found);
    }

    @Test
    public void header_is_the_same_after_rolling_and_reopening() {
        segment.roll(1, false);
        LogHeader original = segment.header;
        segment.close();
        segment = open(testFile);
        LogHeader found = segment.header;
        assertEquals(original, found);
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

    private Segment<String> mergeOutSegment() {
        return new Segment<>(
                testFile,
                StorageMode.MMAP,
                SEGMENT_SIZE,
                Serializers.STRING,
                new BufferPool(MAX_ENTRY_SIZE, false),
                WriteMode.MERGE_OUT,
                CHECKSUM_PROB,
                READ_PAGE_SIZE);
    }

    public static class CachedSegmentTest extends SegmentTest {

        @Override
        Segment<String> open(File file) {
            return new Segment<>(
                    file,
                    StorageMode.RAF_CACHED,
                    SEGMENT_SIZE,
                    Serializers.STRING,
                    new BufferPool(MAX_ENTRY_SIZE, false),
                    WriteMode.LOG_HEAD,
                    CHECKSUM_PROB,
                    READ_PAGE_SIZE);
        }
    }

    public static class MMapSegmentTest extends RafSegmentTest {

        @Override
        Segment<String> open(File file) {
            return new Segment<>(
                    file,
                    StorageMode.MMAP,
                    SEGMENT_SIZE,
                    Serializers.STRING,
                    new BufferPool(MAX_ENTRY_SIZE, false),
                    WriteMode.LOG_HEAD,
                    CHECKSUM_PROB,
                    READ_PAGE_SIZE);
        }
    }

    public static class RafSegmentTest extends SegmentTest {

        @Override
        Segment<String> open(File file) {
            return new Segment<>(
                    file,
                    StorageMode.RAF,
                    SEGMENT_SIZE,
                    Serializers.STRING,
                    new BufferPool(MAX_ENTRY_SIZE, false),
                    WriteMode.LOG_HEAD,
                    CHECKSUM_PROB,
                    READ_PAGE_SIZE);
        }
    }


}