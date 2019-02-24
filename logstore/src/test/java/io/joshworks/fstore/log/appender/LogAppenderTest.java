package io.joshworks.fstore.log.appender;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.io.StorageProvider;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.record.RecordHeader;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.serializer.StringSerializer;
import io.joshworks.fstore.testutils.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public abstract class LogAppenderTest {

    private static final int SEGMENT_SIZE = (int) Size.KB.of(128);//64kb
    private static final int MAX_ENTRY_SIZE = SEGMENT_SIZE;//64kb

    private LogAppender<String> appender;
    private File testDirectory;

    protected abstract LogAppender<String> appender(File testDirectory, int segmentSize);

    public LogAppender<String> appender() {
        return appender(testDirectory, SEGMENT_SIZE);
    }

    @Before
    public void setUp() {
        testDirectory = FileUtils.testFolder();
        testDirectory.deleteOnExit();
        appender = appender();
    }

    @After
    public void cleanup() {
        IOUtils.closeQuietly(appender);
        FileUtils.tryDelete(testDirectory);
    }

    @Test
    public void roll_size_based() {
        int written = 0;
        while (written <= SEGMENT_SIZE) {
            String data = UUID.randomUUID().toString();
            appender.append(data);
            written += data.length();

        }
        appender.append("new-segment");
        assertEquals(2, appender.levels.numSegments());
    }

    @Test
    public void scanner_returns_in_insertion_order_with_multiple_segments() {

        appender.append("a");
        appender.append("b");

        appender.roll();

        appender.append("c");
        appender.flush();

        assertEquals(2, appender.levels.numSegments());

        LogIterator<String> logIterator = appender.iterator(Direction.FORWARD);

        String lastValue = null;

        while (logIterator.hasNext()) {
            lastValue = logIterator.next();
        }

        assertEquals("c", lastValue);
    }

    @Test
    public void positionOnSegment() {

        int segmentIdx = 0;
        long positionOnSegment = 32;
        long position = appender.toSegmentedPosition(segmentIdx, positionOnSegment);

        int segment = appender.getSegment(position);
        long foundPositionOnSegment = appender.getPositionOnSegment(position);

        assertEquals(segmentIdx, segment);
        assertEquals(positionOnSegment, foundPositionOnSegment);
    }

    @Test
    public void get_returns_correct_data_on_single_segment() {
        long pos1 = appender.append("1");
        long pos2 = appender.append("2");

        appender.flush();

        assertEquals("1", appender.get(pos1));
        assertEquals("2", appender.get(pos2));
    }

    @Test
    public void get_returns_correct_data_on_multiple_segments() {
        int entries = 100000;
        long[] positions = new long[entries];
        for (int i = 0; i < entries; i++) {
            positions[i] = appender.append(String.valueOf(i));
        }

        for (int i = 0; i < entries; i++) {
            String found = appender.get(positions[i]);
            assertEquals(String.valueOf(i), found);
        }
    }

    @Test
    public void empty_appender_return_LOG_START_position() {
        assertEquals(Log.START, appender.position());
    }

    @Test
    public void appender_return_correct_position_after_insertion() {

        long pos1 = appender.append("1");
        long pos2 = appender.append("2");
        long pos3 = appender.append("3");

        appender.flush();
        LogIterator<String> logIterator = appender.iterator(Direction.FORWARD);

        assertEquals(pos1, logIterator.position());
        String found = logIterator.next();
        assertEquals("1", found);

        assertEquals(pos2, logIterator.position());
        found = logIterator.next();
        assertEquals("2", found);

        assertEquals(pos3, logIterator.position());
        found = logIterator.next();
        assertEquals("3", found);
    }

    @Test
    public void reader_position() {

        StringBuilder sb = new StringBuilder();
        while (sb.length() <= SEGMENT_SIZE) {
            sb.append(UUID.randomUUID().toString());
        }

        String lastEntry = "FIRST-ENTRY-NEXT-SEGMENT";
        long lastWrittenPosition = appender.append(lastEntry);

        appender.flush();

        LogIterator<String> logIterator = appender.iterator(Direction.FORWARD, lastWrittenPosition);

        assertTrue(logIterator.hasNext());
        assertEquals(lastEntry, logIterator.next());
    }

    @Test
    public void reopen() {

        appender.close();

        long pos1;
        long pos2;
        long pos3;
        long pos4;

        try (LogAppender<String> testAppender = appender()) {
            pos1 = testAppender.append("1");
            pos2 = testAppender.append("2");
            pos3 = testAppender.append("3");
        }

        try (LogAppender<String> testAppender = appender()) {
            pos4 = testAppender.append("4");
        }

        try (LogAppender<String> testAppender = appender()) {
            assertEquals("1", testAppender.get(pos1));
            assertEquals("2", testAppender.get(pos2));
            assertEquals("3", testAppender.get(pos3));
            assertEquals("4", testAppender.get(pos4));

            Set<String> values = testAppender.stream(Direction.FORWARD).collect(Collectors.toSet());
            assertTrue(values.contains("1"));
            assertTrue(values.contains("2"));
            assertTrue(values.contains("3"));
            assertTrue(values.contains("4"));
        }
    }

    @Test
    public void when_reopened_use_metadata_instead_builder_params() {
        appender.append("a");
        appender.append("b");

        assertEquals(2, appender.entries());

        appender.close();

        appender = appender();
        assertEquals(2, appender.entries());
        assertEquals(2, appender.stream(Direction.FORWARD).count());
    }

    @Test
    public void when_reopened_the_index_returns_all_items() {

        int entries = 100000;
        for (int i = 0; i < entries; i++) {
            appender.append(String.valueOf(i));
        }

        appender.close();

        appender = appender();

        Stream<String> stream = appender.stream(Direction.FORWARD);
        assertEquals(entries, stream.count());
    }

    @Test
    public void bad_log_data_is_ignored_when_opening_current_log() throws IOException {
        appender.close();

        String segmentName;
        try (LogAppender<String> testAppender = appender()) {
            testAppender.append("1");
            testAppender.append("2");
            testAppender.append("3");

            //get last segment (in this case there will be always one)
            segmentName = testAppender.currentSegment();
        }

        //write broken data
        File file = new File(testDirectory, segmentName);
        try (Storage storage = StorageProvider.of(StorageMode.RAF).open(file)) {
            storage.writePosition(Log.START);
            ByteBuffer broken = ByteBuffer.allocate(RecordHeader.HEADER_OVERHEAD + 4);
            broken.putInt(444); //expected length
            broken.putInt(123456); // broken checksum
            broken.putChar('A'); // broken data
            broken.putInt(444); //expected length
            broken.flip();

            storage.write(broken);
        }

        try (LogAppender<String> testAppender = appender()) {
            testAppender.append("4");
        }
        try (LogAppender<String> testAppender = appender()) {
            testAppender.append("5");
        }

        try (LogAppender<String> testAppender = appender()) {
            Set<String> values = testAppender.stream(Direction.FORWARD).collect(Collectors.toSet());
            assertThat(values, hasItem("4"));
            assertThat(values, hasItem("5"));
        }
    }

    @Test
    public void segmentBitShift() {
        for (int i = 0; i < appender.MAX_SEGMENTS; i++) {
            long position = appender.toSegmentedPosition(i, 0);
            long foundSegment = appender.getSegment(position);
            assertEquals("Failed on segIdx " + i + " - position: " + position + " - foundSegment: " + foundSegment, i, foundSegment);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void toSegmentedPosition_invalid() {
        long invalidAddress = appender.MAX_SEGMENTS + 1;
        appender.toSegmentedPosition(invalidAddress, 0);

    }

    @Test
    public void getPositionOnSegment() {

        long value = 1;
        long position = appender.getPositionOnSegment(1);
        assertEquals("Failed on position: " + position, value, position);

        value = appender.MAX_SEGMENT_ADDRESS / 2;
        position = appender.getPositionOnSegment(value);
        assertEquals("Failed on position: " + position, value, position);

        value = appender.MAX_SEGMENT_ADDRESS;
        position = appender.getPositionOnSegment(value);
        assertEquals("Failed on position: " + position, value, position);
    }

    @Test
    public void reopen_reads_from_segment_header() {

        appender.close();

        //create
        try (LogAppender<String> testAppender = appender()) {
            Log<String> testSegment = testAppender.current();

            assertTrue(testSegment.created() > 0);
            assertEquals(0, testSegment.entries());
            assertEquals(0, testSegment.level());
            assertFalse(testSegment.readOnly());
        }

        //duplicated code, part of the test, do not delete
        //open
        try (LogAppender<String> testAppender = appender()) {
            Log<String> testSegment = testAppender.current();

            assertTrue(testSegment.created() > 0);
            assertEquals(0, testSegment.entries());
            assertEquals(0, testSegment.level());
            assertFalse(testSegment.readOnly());
        }

        //open
        try (LogAppender<String> testAppender = appender()) {
            Log<String> testSegment = testAppender.current();
            testSegment.append("a");
            testSegment.roll(1);

            assertEquals(1, testSegment.entries());
            assertEquals(1, testSegment.level());
            assertTrue(testSegment.readOnly());

        }
    }

    @Test
    public void get_return_all_items() {
        List<Long> positions = new ArrayList<>();
        int size = 100000;
        for (int i = 0; i < size; i++) {
            long pos = appender.append(String.valueOf(i));
            positions.add(pos);
        }

        for (int i = 0; i < size; i++) {
            String val = appender.get(positions.get(i));
            assertEquals(String.valueOf(i), val);
        }
    }

    @Test
    public void iterator_returns_all_elements() {
        int size = 10000;
        int numSegments = 5;

        for (int i = 0; i < size; i++) {
            appender.append(String.valueOf(i));
            if (i > 0 && i % (size / numSegments) == 0) {
                appender.roll();
            }
        }

        appender.flush();

        assertEquals(size, appender.stream(Direction.FORWARD).count());
        assertEquals(size, appender.entries());

        LogIterator<String> scanner = appender.iterator(Direction.FORWARD);

        int val = 0;
        while (scanner.hasNext()) {
            long pos = scanner.position();
            String next = scanner.next();
            assertEquals("Failed on " + pos, String.valueOf(val++), next);
        }
    }


    @Test
    public void backwards_scanner_returns_all_records() throws IOException {
        int entries = 10;
        for (int i = 0; i < entries; i++) {
            appender.append(String.valueOf(i));
        }

        appender.flush();

        int current = entries - 1;
        try (LogIterator<String> iterator = appender.iterator(Direction.BACKWARD)) {
            while (iterator.hasNext()) {
                String next = iterator.next();
                assertEquals(String.valueOf(current--), next);
            }
        }
        assertEquals(-1, current);
    }

    @Test
    public void backwards_scanner_with_position_returns_all_records() throws IOException {
        int entries = 100000;
        for (int i = 0; i < entries; i++) {
            appender.append(String.valueOf(i));
        }

        appender.flush();

        long position = appender.position();
        for (int i = entries - 1; i >= 0; i--) {
            try (LogIterator<String> iterator = appender.iterator(Direction.BACKWARD, position)) {
                assertTrue("Failed on position " + position, iterator.hasNext());

                String next = iterator.next();
                assertEquals("Failed on position " + position, String.valueOf(i), next);
                position = iterator.position();

            }
        }
    }

    @Test
    public void forward_scanner_with_position_returns_all_records() throws IOException {
        int entries = 100000;
        long position = appender.position();
        for (int i = 0; i < entries; i++) {
            appender.append(String.valueOf(i));
        }

        appender.flush();

        for (int i = 0; i < entries; i++) {
            try (LogIterator<String> iterator = appender.iterator(Direction.FORWARD, position)) {
                assertTrue("Failed on position " + position, iterator.hasNext());

                String next = iterator.next();
                assertEquals("Failed on position " + position, String.valueOf(i), next);
                position = iterator.position();

            } catch (Exception e) {
                System.err.println("Failed on " + i);
                throw e;
            }
        }
    }

    @Test
    public void forward_iterator_position_returns_correct_values() throws IOException {
        int entries = 100000;
        List<Long> positions = new ArrayList<>();
        for (int i = 0; i < entries; i++) {
            long pos = appender.append(String.valueOf(i));
            positions.add(pos);
        }

        appender.flush();

        try (LogIterator<String> iterator = appender.iterator(Direction.FORWARD)) {
            for (int i = 0; i < entries; i++) {
                assertTrue(iterator.hasNext());
                Long position = iterator.position();

                assertEquals(positions.get(i), position);

                iterator.next();
            }
        }
    }

    @Test
    public void backward_iterator_position_returns_correct_values_with_single_segment() throws IOException {
        int entries = 3000; //do not change
        List<Long> positions = new ArrayList<>();
        for (int i = 0; i < entries; i++) {
            long pos = appender.append("value-" + i);
            positions.add(pos);
        }

        appender.flush();

        try (LogIterator<String> iterator = appender.iterator(Direction.BACKWARD)) {
            for (int i = entries; i > 0; i--) {
                assertTrue(iterator.hasNext());
                iterator.next();
                Long position = iterator.position();
                assertEquals("Failed on " + i, positions.get(i - 1), position);

            }
        }
    }

    @Test
    public void backward_iterator_position_returns_correct_values_with_two_segments() throws IOException {
        int entries = 100000; //do not change
        List<Long> positions = new ArrayList<>();
        List<String> values = new ArrayList<>();
        for (int i = 0; i < entries; i++) {
            String val = String.valueOf(i);
            long pos = appender.append(val);
            positions.add(pos);
            values.add(val);
        }

        appender.flush();

        Collections.reverse(positions);
        Collections.reverse(values);
        try (LogIterator<String> iterator = appender.iterator(Direction.BACKWARD)) {
            for (int i = 0; i < entries; i++) {
                assertTrue(iterator.hasNext());

                String val = iterator.next();
                assertEquals("Failed on " + i, values.get(i), val);

                Long position = iterator.position();
                assertEquals("Failed on " + i, positions.get(i), position);
            }
        }
    }

    @Test
    public void backward_iterator_position_returns_correct_values_with_multiple_segments() throws IOException {
        int entries = 200000;
        List<Long> positions = new ArrayList<>();
        for (int i = 0; i < entries; i++) {
            long pos = appender.append("value-" + i);
            positions.add(pos);
        }

        appender.flush();

        try (LogIterator<String> iterator = appender.iterator(Direction.BACKWARD)) {
            for (int i = entries; i > 0; i--) {
                assertTrue(iterator.hasNext());
                iterator.next();
                Long position = iterator.position();
                assertEquals("Failed on " + i, positions.get(i - 1), position);

            }
        }
    }

    @Test
    public void backward_iterator_returns_all_items_after_reopened_appender() throws IOException {
        int entries = 100000;
        List<Long> positions = new ArrayList<>();
        for (int i = 0; i < entries; i++) {
            long pos = appender.append("value-" + i);
            positions.add(pos);
        }

        appender.close();
        appender = appender();

        try (LogIterator<String> iterator = appender.iterator(Direction.BACKWARD)) {
            for (int i = entries; i > 0; i--) {
                assertTrue(iterator.hasNext());
                iterator.next();
                Long position = iterator.position();
                assertEquals("Failed on " + i, positions.get(i - 1), position);

            }
        }
    }

    @Test
    public void forward_iterator_returns_all_items_after_reopened_appender() throws IOException {
        int entries = 10000;
        List<Long> positions = new ArrayList<>();
        for (int i = 0; i < entries; i++) {
            long pos = appender.append("value-" + i);
            positions.add(pos);
        }

        appender.close();
        appender = appender();

        try (LogIterator<String> iterator = appender.iterator(Direction.FORWARD)) {
            for (int i = 0; i < entries; i++) {
                assertTrue(iterator.hasNext());
                Long position = iterator.position();
                iterator.next();
                assertEquals("Failed on " + i, positions.get(i), position);
            }
        }
    }

    @Test
    public void position_is_the_same_after_reopening() {
        int entries = 100000; //must be more than a single segment
        for (int i = 0; i < entries; i++) {
            appender.append("value-" + i);
        }

        appender.flush();

        long prev = appender.position();
        appender.close();
        appender = appender();

        assertEquals(prev, appender.position());
    }

    @Test
    public void forward_reader_returns_data_after_rolling_segment() {

        LogIterator<String> iterator = appender.iterator(Direction.FORWARD);

        appender.append("a");
        assertTrue(iterator.hasNext());
        assertEquals("a", iterator.next());

        assertFalse(iterator.hasNext());
        appender.roll();
        assertFalse(iterator.hasNext());

        appender.append("b");
        assertTrue(iterator.hasNext());
        assertEquals("b", iterator.next());
    }

    @Test
    public void entries_return_same_value_after_reopening() {
        appender.append("a");
        appender.append("b");
        assertEquals(2, appender.entries());

        appender.close();
        appender = appender();
        assertEquals(2, appender.entries());
    }

    @Test
    public void entries_return_same_value_after_reopening_with_multiple_segments() {
        appender.append("a");
        appender.append("b");
        assertEquals(2, appender.entries());

        appender.roll();
        appender.append("c");
        assertEquals(3, appender.entries());

        appender.close();
        appender = appender();
        assertEquals(3, appender.entries());
    }

    @Test
    public void position_return_same_value_after_reopening() {
        appender.append("a");
        long pos = appender.position();
        appender.close();
        appender = appender();
        assertEquals(pos, appender.position());
    }

    @Test
    public void position_return_same_value_after_reopening_with_multiple_segments() {
        appender.append("a");

        appender.roll();
        appender.append("b");

        long pos = appender.position();
        appender.close();
        appender = appender();
        assertEquals(pos, appender.position());
    }


    public static class MMapLogAppenderTest extends LogAppenderTest {

        @Override
        protected LogAppender<String> appender(File testDirectory, int segmentSize) {
            return LogAppender.builder(testDirectory, new StringSerializer())
                    .segmentSize(segmentSize)
                    .storageMode(StorageMode.MMAP)
                    .maxEntrySize(MAX_ENTRY_SIZE)
                    .disableCompaction()
                    .open();
        }
    }

    public static class CachedRafLogAppenderTest extends LogAppenderTest {

        @Override
        protected LogAppender<String> appender(File testDirectory, int segmentSize) {
            return LogAppender.builder(testDirectory, new StringSerializer())
                    .segmentSize(segmentSize)
                    .storageMode(StorageMode.RAF_CACHED)
                    .maxEntrySize(MAX_ENTRY_SIZE)
                    .disableCompaction()
                    .open();
        }
    }

    public static class RafLogAppenderTest extends LogAppenderTest {


        @Override
        protected LogAppender<String> appender(File testDirectory, int segmentSize) {
            return LogAppender.builder(testDirectory, new StringSerializer())
                    .segmentSize(segmentSize)
                    .maxEntrySize(MAX_ENTRY_SIZE)
                    .disableCompaction()
                    .open();
        }
    }

}