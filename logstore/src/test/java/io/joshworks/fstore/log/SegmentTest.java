package io.joshworks.fstore.log;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.io.StorageProvider;
import io.joshworks.fstore.core.io.buffers.SingleBufferThreadCachedPool;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.record.DataStream;
import io.joshworks.fstore.log.record.RecordHeader;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.Segment;
import io.joshworks.fstore.log.segment.header.Type;
import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.fstore.testutils.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public abstract class SegmentTest {

    protected static final long MAX_ENTRY_SIZE = 1024 * 1024 * 5L;
    protected static final double CHCKSUM_PROB = 1;

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

    @Test
    public void segment_expands_larger_than_original_size() {
        long originalSize = segment.fileSize();
        while (segment.logicalSize() <= originalSize) {
            segment.append("a");
        }

        segment.append("a");

        assertTrue(segment.fileSize() > originalSize);
    }

    @Test
    public void writePosition_reopen() throws IOException {
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
    public void reader_reopen() throws IOException {
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
        for (int i = 0; i < MAX_ENTRY_SIZE - RecordHeader.HEADER_OVERHEAD; i++) {
            sb.append("a");
        }
        String data = sb.toString();
        segment.append(data);

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
            testSegment.roll(1);

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
    public void scanner_0() {
        testScanner(0);
    }

    @Test
    public void scanner_1() {
        testScanner(1);
    }

    @Test
    public void scanner_10() {
        testScanner(10);
    }

    @Test
    public void scanner_1000() {
        testScanner(1000);
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
        int entries = 100000;
        for (int i = 0; i < entries; i++) {
            long pos = segment.append(String.valueOf(i));
            assertTrue("Entry was not inserted", pos > 0);
        }

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
        int entries = 300000;
        List<Long> positions = new ArrayList<>();
        for (int i = 0; i < entries; i++) {
            long pos = segment.append(String.valueOf(i));
            positions.add(pos);
        }
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
        int entries = 300000;
        List<Long> positions = new ArrayList<>();
        for (int i = 0; i < entries; i++) {
            long pos = segment.append(String.valueOf(i));
            assertTrue("Entry was not inserted", pos > 0);
            positions.add(pos);
        }

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

    private void testScanner(int items) {
        List<String> values = new ArrayList<>();
        for (int i = 0; i < items; i++) {
            String value = UUID.randomUUID().toString();
            values.add(value);
            segment.append(value);
        }
        segment.flush();

        int i = 0;

        LogIterator<String> logIterator = segment.iterator(Direction.FORWARD);
        while (logIterator.hasNext()) {
            assertEquals("Failed on iteration " + i, values.get(i), logIterator.next());
            i++;
        }
        assertEquals(items, i);
    }

    public static class CachedSegmentTest extends SegmentTest {

        @Override
        Log<String> open(File file) {
            return new Segment<>(
                    StorageProvider.of(StorageMode.RAF_CACHED).create(file, Size.MB.of(10)),
                    Serializers.STRING,
                    new DataStream(new SingleBufferThreadCachedPool(false), CHCKSUM_PROB, MAX_ENTRY_SIZE),
                    "magic",
                    Type.LOG_HEAD);
        }

    }

    public static class MMapSegmentTest extends RafSegmentTest {

        @Override
        Log<String> open(File file) {
            return new Segment<>(
                    StorageProvider.of(StorageMode.MMAP).create(file, Size.MB.of(10)),
                    Serializers.STRING,
                    new DataStream(new SingleBufferThreadCachedPool(false), CHCKSUM_PROB, MAX_ENTRY_SIZE), "magic", Type.LOG_HEAD);
        }
    }

    public static class RafSegmentTest extends SegmentTest {

        @Override
        Log<String> open(File file) {
            return new Segment<>(
                    StorageProvider.of(StorageMode.RAF).create(file, Size.MB.of(10)),
                    Serializers.STRING,
                    new DataStream(new SingleBufferThreadCachedPool(false), CHCKSUM_PROB, MAX_ENTRY_SIZE),
                    "magic",
                    Type.LOG_HEAD);
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