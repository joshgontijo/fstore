package io.joshworks.eventry.index;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.PollingSubscriber;
import io.joshworks.fstore.testutils.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TableIndexTest {

    private TableIndex tableIndex;
    private File testDirectory;
    private static final int FLUSH_THRESHOLD = 1000000;
    private static final boolean USE_COMPRESSION = true;

    @Before
    public void setUp() {
        testDirectory = FileUtils.testFolder();
        tableIndex = new TableIndex(testDirectory, FLUSH_THRESHOLD, USE_COMPRESSION);
    }

    @After
    public void tearDown() {
        IOUtils.closeQuietly(tableIndex);
        FileUtils.tryDelete(testDirectory);
    }

    @Test
    public void event_version_is_in_ascending_order() {

        long stream = 1;

        tableIndex.add(stream, 0, 0);
        tableIndex.flush();
        tableIndex.add(stream, 1, 0);
        tableIndex.flush();
        tableIndex.add(stream, 2, 0);
        tableIndex.flush();
        tableIndex.add(stream, 3, 0); //memory

        LogIterator<IndexEntry> it = tableIndex.iterator(Direction.FORWARD, Range.anyOf(stream));

        assertEquals(0, it.next().version);
        assertEquals(1, it.next().version);
        assertEquals(2, it.next().version);
        assertEquals(3, it.next().version);
    }

    @Test
    public void data_added_to_the_inMemory_can_be_retrieved() {

        long stream = 1;
        int version = 1;
        tableIndex.add(stream, version, 0);
        Optional<IndexEntry> indexEntry = tableIndex.get(stream, version);

        assertTrue(indexEntry.isPresent());
        assertEquals(stream, indexEntry.get().stream);
        assertEquals(version, indexEntry.get().version);
    }

    @Test
    public void data_added_to_the_disk_is_retrieved() {

        long stream = 1;
        int version = 0;
        tableIndex.add(stream, version, 0);

        tableIndex.flush();

        Optional<IndexEntry> indexEntry = tableIndex.get(stream, version);

        assertTrue(indexEntry.isPresent());
        assertEquals(stream, indexEntry.get().stream);
        assertEquals(version, indexEntry.get().version);
    }

    @Test
    public void version_added_to_memory_is_retrieved() {
        long stream = 1;
        int version = 1;
        tableIndex.add(stream, version, 0);

        int foundVersion = tableIndex.version(stream);

        assertEquals(version, foundVersion);
    }

    @Test
    public void version_added_to_disk_is_retrieved() {
        long stream = 1;
        int version = 0;
        tableIndex.add(stream, version, 0);

        tableIndex.flush();

        int foundVersion = tableIndex.version(stream);

        assertEquals(version, foundVersion);
    }

    @Test
    public void size_returns_the_total_of_inMemory_and_disk() {
        long stream = 1;
        tableIndex.add(stream, 1, 0);

        tableIndex.flush();

        tableIndex.add(stream, 2, 0);

        long size = tableIndex.size();

        assertEquals(2, size);
    }

    @Test
    public void stream_returns_data_from_inMemory_and_disk() {
        long stream = 1;
        tableIndex.add(stream, 1, 0);

        tableIndex.flush();

        tableIndex.add(stream, 2, 0);

        Stream<IndexEntry> dataStream = tableIndex.stream(Direction.FORWARD, Range.anyOf(stream));

        assertEquals(2, dataStream.count());
    }

    @Test
    public void range_stream_with_range_returns_data_from_disk_and_memory() {
        long stream = 1;

        tableIndex.add(stream, 0, 0);
        tableIndex.flush();
        tableIndex.add(stream, 1, 0);

        Stream<IndexEntry> dataStream = tableIndex.stream(Direction.FORWARD, Range.anyOf(stream));

        assertEquals(2, dataStream.count());
    }

    @Test
    public void stream_returns_data_from_disk_and_memory() {

        //given
        long stream = 1;
        //2 segments + in memory
        int size = (FLUSH_THRESHOLD * 2) + FLUSH_THRESHOLD / 2;
        for (int i = 0; i < size; i++) {
            tableIndex.add(stream, i, 0);
        }

        Stream<IndexEntry> dataStream = tableIndex.stream(Direction.FORWARD, Range.anyOf(stream));

        assertEquals(size, dataStream.count());
    }

    @Test
    public void iterator_runs_forward_in_the_log() {

        //given
        long stream = 1;
        //2 segments + in memory
        int size = (FLUSH_THRESHOLD * 2) + FLUSH_THRESHOLD / 2;
        for (int i = 0; i < size; i++) {
            tableIndex.add(stream, i, 0);
        }

        LogIterator<IndexEntry> it = tableIndex.iterator(Direction.FORWARD, Range.anyOf(stream));

        int expectedVersion = 0;
        while (it.hasNext()) {
            IndexEntry next = it.next();
            assertEquals(expectedVersion, next.version);
            expectedVersion = next.version + 1;
        }
    }

    @Test
    public void iterator_with_range_runs_forward_in_the_log() {

        //given
        long stream = 1;
        //2 segments + in memory
        int size = (FLUSH_THRESHOLD * 2) + FLUSH_THRESHOLD / 2;
        for (int i = 0; i < size; i++) {
            tableIndex.add(stream, i, 0);
        }

        LogIterator<IndexEntry> it = tableIndex.iterator(Direction.FORWARD, Range.anyOf(stream));

        int expectedVersion = 0;
        while (it.hasNext()) {
            IndexEntry next = it.next();
            assertEquals(expectedVersion, next.version);
            expectedVersion = next.version + 1;
        }
    }

    @Test
    public void stream_with_range_returns_data_from_disk_and_memory() {

        //given
        long stream = 1;
        //2 segments + in memory
        int size = (FLUSH_THRESHOLD * 2) + FLUSH_THRESHOLD / 2;
        for (int i = 0; i < size; i++) {
            tableIndex.add(stream, i, 0);
        }

        Stream<IndexEntry> dataStream = tableIndex.stream(Direction.FORWARD, Range.anyOf(stream));
        assertEquals(size, dataStream.count());
    }


    @Test
    public void reopened_index_returns_all_items_for_stream_rang() {

        //given
        long stream = 1;
        //1 segment + in memory
        int size = FLUSH_THRESHOLD + (FLUSH_THRESHOLD / 2);
        for (int i = 0; i <= size; i++) {
            tableIndex.add(stream, i, 0);
        }

        tableIndex.close();

        tableIndex = new TableIndex(testDirectory, FLUSH_THRESHOLD, USE_COMPRESSION);

        Stream<IndexEntry> dataStream = tableIndex.stream(Direction.FORWARD, Range.of(stream, 1, 11));

        assertEquals(10, dataStream.count());

        LogIterator<IndexEntry> it = tableIndex.iterator(Direction.FORWARD, Range.of(stream, 1, 11));

        assertEquals(1, it.next().version);
        assertEquals(2, it.next().version);
        assertEquals(3, it.next().version);
        assertEquals(4, it.next().version);
        assertEquals(5, it.next().version);
        assertEquals(6, it.next().version);
        assertEquals(7, it.next().version);
        assertEquals(8, it.next().version);
        assertEquals(9, it.next().version);
        assertEquals(10, it.next().version);

    }

    @Test
    public void version_is_returned() {

        tableIndex.close();

        //given
        int streams = 100000;
        try (TableIndex index = new TableIndex(testDirectory, 500000, USE_COMPRESSION)) {

            for (int i = 0; i < streams; i++) {
                index.add(i, 1, 0);
            }

            for (int i = 0; i < streams; i++) {
                //when
                int version = index.version(i);
                //then
                assertEquals(1, version);
            }
        }
    }

    @Test
    public void version_is_minus_one_for_non_existing_stream() {
        int version = tableIndex.version(1234);
        assertEquals(IndexEntry.NO_VERSION, version);
    }

    @Test
    public void poll_returns_data_from_memory() throws IOException, InterruptedException {
        int entries = 500;
        long stream = 123L;
        for (int i = 0; i < entries; i++) {
            tableIndex.add(stream, i, 0);
        }

        try (PollingSubscriber<IndexEntry> poller = tableIndex.poller(stream)) {

            for (int i = 0; i < entries; i++) {
                IndexEntry poll = poller.poll();
                assertNotNull(poll);
                assertEquals(i, poll.version);
            }

        }
    }

    @Test
    public void poll_returns_data_from_multiple_streams() throws IOException, InterruptedException {
        int entries = 1500000;
        Set<Long> streams = new HashSet<>();
        for (long i = 0; i < entries; i++) {
            streams.add(i);
            tableIndex.add(i, 0, 0);
        }

        try (PollingSubscriber<IndexEntry> poller = tableIndex.poller(streams)) {

            for (int i = 0; i < entries; i++) {
                IndexEntry poll = poller.poll();
                assertNotNull(poll);
                assertTrue(streams.contains(poll.stream));
                assertEquals(0, poll.version);
            }
        }
    }

    @Test
    public void poll_returns_data_from_multiple_streams_in_order_per_stream() throws IOException, InterruptedException {
        int streams = 10000;
        int versions = 100;

        Map<Long, Integer> versionTracker = new HashMap<>();
        for (long stream = 0; stream < streams; stream++) {
            versionTracker.put(stream, 0);
            for (int version = 0; version < versions; version++) {
                tableIndex.add(stream, version, 0);
            }
        }

        try (PollingSubscriber<IndexEntry> poller = tableIndex.poller(versionTracker.keySet())) {

            for (long stream = 0; stream < streams; stream++) {
                for (int version = 0; version < versions; version++) {
                    IndexEntry poll = poller.take();
                    assertNotNull(poll);
                    assertTrue(versionTracker.containsKey(poll.stream));
                    assertEquals((int) versionTracker.get(poll.stream), poll.version);
                    versionTracker.put(poll.stream, poll.version + 1);
                }
            }

        }
    }

    @Test
    public void poll_returns_data_from_disk() throws IOException, InterruptedException {
        int entries = 500;
        long stream = 123L;
        for (int i = 0; i < entries; i++) {
            tableIndex.add(stream, i, 0);
        }

        tableIndex.flush();

        try (PollingSubscriber<IndexEntry> poller = tableIndex.poller(stream)) {

            for (int i = 0; i < entries; i++) {
                IndexEntry poll = poller.poll();
                assertNotNull(poll);
                assertEquals(i, poll.version);
            }

        }
    }

    @Test
    public void take_returns_data_from_disk() throws IOException, InterruptedException {
        int entries = 10;
        long stream = 123L;
        for (int i = 0; i < entries; i++) {
            tableIndex.add(stream, i, 0);
        }

        tableIndex.flush();

        try (PollingSubscriber<IndexEntry> poller = tableIndex.poller(stream)) {

            for (int i = 0; i < entries; i++) {
                IndexEntry poll = poller.take();
                assertNotNull(poll);
                assertEquals(i, poll.version);
            }

        }
    }

    @Test
    public void peek_doesnt_advance_inmemory_items() throws IOException, InterruptedException {
        long stream = 123L;
        tableIndex.add(stream, 0, 0);
        tableIndex.add(stream, 1, 0);

        try (PollingSubscriber<IndexEntry> poller = tableIndex.poller(stream)) {

            for (int i = 0; i < 10; i++) {
                IndexEntry peek = poller.peek();
                assertEquals(0, peek.version);
            }

            IndexEntry poll = poller.poll();
            assertEquals(0, poll.version);
        }
    }

    @Test
    public void peek_doesnt_advance_disk_items() throws IOException, InterruptedException {
        long stream = 123L;
        tableIndex.add(stream, 0, 0);
        tableIndex.add(stream, 1, 0);

        tableIndex.flush();

        try (PollingSubscriber<IndexEntry> poller = tableIndex.poller(stream)) {

            for (int i = 0; i < 10; i++) {
                IndexEntry peek = poller.peek();
                assertEquals(0, peek.version);
            }

            IndexEntry poll = poller.poll();
            assertNotNull(poll);
            assertEquals(0, poll.version);

        }
    }

    @Test
    public void poll_returns_data_from_disk_and_memory_if_available() throws IOException, InterruptedException {
        int entries = 10;
        long stream = 123L;

        for (int i = 0; i < entries; i++) {
            tableIndex.add(stream, i, 0);
        }
        tableIndex.flush();

        for (int i = entries; i < entries * 2; i++) {
            tableIndex.add(stream, i, 0);
        }

        try (PollingSubscriber<IndexEntry> poller = tableIndex.poller(stream)) {

            for (int i = 0; i < entries * 2; i++) {
                IndexEntry poll = poller.poll();
                assertNotNull(poll);
                assertEquals(i, poll.version);
            }
        }
    }

    @Test
    public void poll_returns_null_if_no_inmemory_stream_matches() throws IOException, InterruptedException {
        int entries = 10;
        long stream = 123L;
        long someOtherStream = 456L;

        for (int i = 0; i < entries; i++) {
            tableIndex.add(stream, i, 0);
        }

        try (PollingSubscriber<IndexEntry> poller = tableIndex.poller(someOtherStream)) {
            for (int i = 0; i < entries; i++) {
                IndexEntry poll = poller.poll();
                assertNull(poll);
            }
        }
    }

    @Test
    public void poll_returns_null_if_no_disk_stream_matches() throws IOException, InterruptedException {
        int entries = 10;
        long stream = 123L;
        long someOtherStream = 456L;

        for (int i = 0; i < entries; i++) {
            tableIndex.add(stream, i, 0);
        }

        tableIndex.flush();

        try (PollingSubscriber<IndexEntry> poller = tableIndex.poller(someOtherStream)) {
            for (int i = 0; i < entries; i++) {
                IndexEntry poll = poller.poll();
                assertNull(poll);
            }
        }
    }

    @Test
    public void peek_returns_null_if_no_disk_stream_matches() throws IOException, InterruptedException {
        int entries = 10;
        long stream = 123L;
        long someOtherStream = 456L;

        for (int i = 0; i < entries; i++) {
            tableIndex.add(stream, i, 0);
        }

        tableIndex.flush();

        try (PollingSubscriber<IndexEntry> poller = tableIndex.poller(someOtherStream)) {
            for (int i = 0; i < entries; i++) {
                IndexEntry poll = poller.peek();
                assertNull(poll);
            }
        }
    }

    @Test
    public void peek_returns_null_if_no_inmemory_stream_matches() throws IOException, InterruptedException {
        int entries = 10;
        long stream = 123L;
        long someOtherStream = 456L;

        for (int i = 0; i < entries; i++) {
            tableIndex.add(stream, i, 0);
        }

        try (PollingSubscriber<IndexEntry> poller = tableIndex.poller(someOtherStream)) {
            for (int i = 0; i < entries; i++) {
                IndexEntry poll = poller.peek();
                assertNull(poll);
            }
        }
    }

    @Test
    public void take_blocks_inmemory_until_stream_matches() throws IOException, InterruptedException {
        int entries = 10;
        long someOtherStream = 456L;
        long expectedStream = 123L;

        for (int i = 0; i < entries; i++) {
            tableIndex.add(someOtherStream, i, 0);
        }

        new Thread(() -> {
            try {
                Thread.sleep(3000);
                tableIndex.add(expectedStream, 0, 0);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();

        try (PollingSubscriber<IndexEntry> poller = tableIndex.poller(expectedStream)) {
            IndexEntry poll = poller.take();
            assertNotNull(poll);
            assertEquals(expectedStream, poll.stream);
            assertEquals(0, poll.version);
        }
    }

    @Test
    public void concurrent_write_and_poller_returns_data_in_sequence() throws IOException, InterruptedException {
        int totalEntries = 2500000;

        long stream = 123L;
        Thread writeThread = new Thread(() -> {
            for (int i = 0; i < totalEntries; i++) {
                tableIndex.add(stream, i, 0);
            }
            System.out.println("COMPLETED WRITE");
        });
        writeThread.start();

        try (PollingSubscriber<IndexEntry> poller = tableIndex.poller(stream)) {
            for (int i = 0; i < totalEntries; i++) {
                IndexEntry entry = poller.take();
                if (i % 100000 == 0) {
                    System.out.println("Polled " + i);
                }
                assertNotNull("Failed on " + i + ": " + entry, entry);
                assertEquals("Failed on " + i + ": " + entry, i, entry.version);
            }
            System.out.println("COMPLETED READ");
        }

        writeThread.join();
    }

    @Test
    public void concurrent_write_and_poller_returns_data_in_sequence_from_multiple_streams() throws IOException, InterruptedException {
        int numVersion = 500;
        long numStreams = 10000;
        Thread writeThread = new Thread(() -> {
            for (int stream = 0; stream < numStreams; stream++) {
                for (int version = 0; version < numVersion; version++) {
                    tableIndex.add(stream, version, 0);
                }
            }
            System.out.println("COMPLETED WRITE");
        });
        writeThread.start();

        Set<Long> streams = LongStream.range(0, numStreams).boxed().collect(Collectors.toSet());
        try (PollingSubscriber<IndexEntry> poller = tableIndex.poller(streams)) {
            for (int i = 0; i < numVersion * numStreams; i++) {
                IndexEntry entry = poller.take();
                if (i % 100000 == 0) {
                    System.out.println("Polled " + i);
                }
            }
            System.out.println("COMPLETED READ");
        }

        writeThread.join();
    }

    @Test
    public void mem_disk_transition_returns_correct_index_order() throws InterruptedException {

        long stream1 = 123L;
        long stream2 = 456L;

        tableIndex.add(stream2, 0, 0);
        tableIndex.flush();
        tableIndex.add(stream2, 1, 0);

        PollingSubscriber<IndexEntry> poller = tableIndex.poller(Set.of(stream1, stream2));

        IndexEntry polled = poller.poll();
        assertEquals(stream2, polled.stream);
        assertEquals(0, polled.version);

        polled = poller.poll();
        assertEquals(stream2, polled.stream);
        assertEquals(1, polled.version);
    }

    @Test
    public void disk_switches_to_mem_poller() throws InterruptedException {

        long stream = 456L;

        tableIndex.add(stream, 0, 0);
        tableIndex.add(stream, 1, 0);

        PollingSubscriber<IndexEntry> poller = tableIndex.poller(stream);

        IndexEntry polled = poller.poll();
        assertEquals(stream, polled.stream);
        assertEquals(0, polled.version);

        tableIndex.flush();

        polled = poller.poll();
        assertEquals(stream, polled.stream);
        assertEquals(1, polled.version);
    }

    @Test
    public void mem_switches_to_disk_poller() throws InterruptedException {

        long stream = 456L;

        tableIndex.add(stream, 0, 0);

        PollingSubscriber<IndexEntry> poller = tableIndex.poller(Set.of(stream));

        IndexEntry polled = poller.poll();
        assertEquals(stream, polled.stream);
        assertEquals(0, polled.version);

        tableIndex.add(stream, 1, 0);
        tableIndex.flush();

        polled = poller.poll();
        assertEquals(stream, polled.stream);
        assertEquals(1, polled.version);
    }

    @Test
    public void poller_returns_the_processed_stream_versions() {
        int versions = 10;
        long stream = 456L;

        for (int version = 0; version < versions; version++) {
            tableIndex.add(stream, version, 0);
        }

        TableIndex.IndexPoller poller = tableIndex.poller(stream);
        IndexEntry poll = poller.poll();
        assertEquals(0, poll.version);

        Map<Long, Integer> processed = poller.processed();
        assertEquals(Integer.valueOf(0), processed.get(stream));

    }

    @Test
    public void poller_starts_from_the_provided_stream_versions() {
        int versions = 10;
        long stream = 456L;

        for (int version = 0; version < versions; version++) {
            tableIndex.add(stream, version, 0);
        }

        TableIndex.IndexPoller poller = tableIndex.poller(stream);
        IndexEntry entry = poller.poll();
        assertEquals(0, entry.version);

        Map<Long, Integer> processed = poller.processed();
        assertEquals(Integer.valueOf(0), processed.get(stream));

        poller.close();

        poller = tableIndex.poller(processed);
        entry = poller.poll();
        assertEquals(1, entry.version);


    }

}