package io.joshworks.fstore.log;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.log.record.DataStream;
import io.joshworks.fstore.log.record.RecordHeader;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.testutils.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class SegmentTest {

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
        for (int i = 0; i < DataStream.MAX_ENTRY_SIZE - RecordHeader.HEADER_OVERHEAD; i++) {
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
            positions.add(segment.append(String.valueOf(i)));
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
    public void scanner_ends_before_footer() {

        int numEntries = 10;
        for (int i = 0; i < numEntries; i++) {
            segment.append(String.valueOf(i));
        }
        segment.flush();

        segment.roll(1);

        Stream<String> stream = segment.stream(Direction.FORWARD);
        assertEquals(numEntries, stream.count());
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

            for (int i = 0; i < 100; i++) {
                testSegment.append("a");
            }

            LogIterator<String> reader = testSegment.iterator(Direction.FORWARD);
            new Thread(() -> {
                while (reader.hasNext()) {
                    String next = reader.next();
                    try {
                        Thread.sleep(50);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }).start();

            testSegment.delete();

        } catch (Exception e) {
            FileUtils.tryDelete(file);
        }
    }

    @Test
    public void segment_read_backwards() throws IOException {
        int entries = 1000000;
        for (int i = 0; i < entries; i++) {
            segment.append(String.valueOf(i));
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
    public void reader_forward_maintain_correct_position() throws IOException {
        int entries = 1000000;
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
        int entries = 1000000;
        List<Long> positions = new ArrayList<>();
        for (int i = 0; i < entries; i++) {
            long pos = segment.append(String.valueOf(i));
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

    @Test
    public void poller_notifies_awaiting_consumers() throws InterruptedException {
        final String value = "yolo";
        int numOfSubscribers = 3;
        ExecutorService executor = Executors.newFixedThreadPool(numOfSubscribers);

        final List<String> captured = new ArrayList<>();

        for (int i = 0; i < numOfSubscribers; i++) {
            executor.submit(() -> {
                try {
                    PollingSubscriber<String> poller = segment.poller();
                    String polled = poller.take();
                    if (polled != null)
                        captured.add(polled);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        executor.shutdown();

        Thread.sleep(4000);

        segment.append(value);
        segment.flush();

        executor.awaitTermination(5, TimeUnit.SECONDS);

        assertEquals(numOfSubscribers, captured.size());

    }

    @Test
    public void poller_doesnt_no_block_when_data_is_available() throws InterruptedException {
        final String value = "yolo";

        segment.append(value);
        segment.flush();

        PollingSubscriber<String> poller = segment.poller();
        String polled = poller.poll(3, TimeUnit.SECONDS);

        assertEquals(value, polled);
    }

    @Test
    public void poll_returns_null_when_segment_is_rolled() throws InterruptedException {

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<String> captured = new AtomicReference<>("NON-NULL");

        segment.append("value");
        segment.flush();
        long position = segment.position();

        new Thread(() -> {
            try {
                PollingSubscriber<String> poller = segment.poller(position);
                String polled = poller.poll();
                captured.set(polled);
                latch.countDown();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();


        segment.roll(1);

        if (!latch.await(5, TimeUnit.DAYS)) {
            fail("Thread was not released");
        }
        assertNull(captured.get());
    }

    @Test
    public void poll_returns_null_when_segment_is_closed() throws InterruptedException, IOException {

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<String> captured = new AtomicReference<>("NON-NULL");

        new Thread(() -> {
            try {
                PollingSubscriber<String> poller = segment.poller();
                String polled = poller.poll();
                captured.set(polled);
                latch.countDown();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();

        Thread.sleep(1000);
        segment.close();

        if (!latch.await(5, TimeUnit.SECONDS)) {
            fail("Thread was not released");
        }
        assertNull(captured.get());
    }

    @Test
    public void poll_headOfLog_returns_true_when_no_data_is_available() {

        PollingSubscriber<String> poller = segment.poller();
        assertTrue(poller.headOfLog());
        segment.append("a");
        segment.flush();
        assertFalse(poller.headOfLog());
    }

    @Test
    public void poll_endOfLog_always_returns_false_only_when_segment_is_closed() throws InterruptedException {

        PollingSubscriber<String> poller = segment.poller();
        assertFalse(poller.endOfLog());

        segment.append("a");
        assertFalse(poller.endOfLog());

        segment.roll(1);
        assertFalse(poller.endOfLog());

        poller.poll();
        assertTrue(poller.endOfLog());
    }
}