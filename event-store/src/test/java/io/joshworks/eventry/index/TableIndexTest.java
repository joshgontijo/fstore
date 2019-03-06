package io.joshworks.eventry.index;

import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.fstore.codec.snappy.SnappyCodec;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.iterators.Iterators;
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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TableIndexTest {

    private TableIndex tableIndex;
    private File testDirectory;
    private static final int FLUSH_THRESHOLD = 1000000;

    @Before
    public void setUp() {
        testDirectory = FileUtils.testFolder();
        tableIndex = new TableIndex(testDirectory, e -> dummyMetadata(), fi -> {
        }, FLUSH_THRESHOLD, 1, new SnappyCodec());
    }

    @After
    public void tearDown() {
        IOUtils.closeQuietly(tableIndex);
        FileUtils.tryDelete(testDirectory);
    }

    private static StreamMetadata dummyMetadata() {
        return new StreamMetadata("dummy", 123, 0, -1, -1, -1, new HashMap<>(), new HashMap<>(), StreamMetadata.STREAM_ACTIVE);
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

        LogIterator<IndexEntry> it = tableIndex.indexedIterator(Checkpoint.of(stream));

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
    public void range_stream_with_range_returns_data_from_disk_and_memory() {
        long stream = 1;

        tableIndex.add(stream, 0, 0);
        tableIndex.flush();
        tableIndex.add(stream, 1, 0);

        Stream<IndexEntry> dataStream = tableIndex.indexedIterator(Checkpoint.of(stream)).stream();
        assertEquals(2, dataStream.count());
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

        LogIterator<IndexEntry> it = tableIndex.indexedIterator(Checkpoint.of(stream));

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

        Stream<IndexEntry> dataStream = tableIndex.indexedIterator(Checkpoint.of(stream)).stream();
        assertEquals(size, dataStream.count());
    }


    @Test
    public void version_is_returned() {

        tableIndex.close();

        //given
        int streams = 100000;
        try (TableIndex index = new TableIndex(testDirectory, e -> null, fi -> {
        }, 500000, 1, new SnappyCodec())) {

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
    public void hasNext_returns_false_when_no_data_is_avaialble() {
        IndexIterator iterator = tableIndex.indexedIterator(Checkpoint.of(1));
        assertFalse(iterator.hasNext());
    }

    @Test
    public void hasNext_returns_true_when_data_is_available() {
        tableIndex.add(1, 0, 0);
        IndexIterator iterator = tableIndex.indexedIterator(Checkpoint.of(1));
        assertTrue(iterator.hasNext());
    }

    @Test
    public void next_returns_data_from_memory() throws IOException {
        int entries = 500;
        long stream = 123L;
        for (int i = 0; i < entries; i++) {
            tableIndex.add(stream, i, 0);
        }

        try (IndexIterator iterator = tableIndex.indexedIterator(Checkpoint.of(stream))) {

            for (int i = 0; i < entries; i++) {
                IndexEntry poll = iterator.next();
                assertNotNull(poll);
                assertEquals(i, poll.version);
            }

        }
    }

    @Test
    public void next_returns_data_from_multiple_streams() throws IOException {
        int entries = 1500000;
        Set<Long> streams = new HashSet<>();
        for (long i = 0; i < entries; i++) {
            streams.add(i);
            tableIndex.add(i, 0, 0);
        }

        try (IndexIterator iterator = tableIndex.indexedIterator(Checkpoint.of(streams))) {

            for (int i = 0; i < entries; i++) {
                IndexEntry poll = iterator.next();
                assertNotNull(poll);
                assertTrue(streams.contains(poll.stream));
                assertEquals(0, poll.version);
            }
        }
    }

    @Test
    public void next_returns_data_from_multiple_streams_in_order_per_stream() throws IOException {
        int streams = 10000;
        int versions = 100;

        Map<Long, Integer> versionTracker = new HashMap<>();
        for (long stream = 0; stream < streams; stream++) {
            versionTracker.put(stream, 0);
            for (int version = 0; version < versions; version++) {
                tableIndex.add(stream, version, 0);
            }
        }

        try (IndexIterator iterator = tableIndex.indexedIterator(Checkpoint.of(versionTracker.keySet()))) {

            for (long stream = 0; stream < streams; stream++) {
                for (int version = 0; version < versions; version++) {
                    IndexEntry poll = iterator.next();
                    assertNotNull(poll);
                    assertTrue(versionTracker.containsKey(poll.stream));
                    assertEquals((int) versionTracker.get(poll.stream), poll.version);
                    versionTracker.put(poll.stream, poll.version + 1);
                }
            }

        }
    }

    @Test
    public void next_returns_data_from_disk() throws IOException {
        int entries = 500;
        long stream = 123L;
        for (int i = 0; i < entries; i++) {
            tableIndex.add(stream, i, 0);
        }

        tableIndex.flush();

        try (IndexIterator iterator = tableIndex.indexedIterator(Checkpoint.of(stream))) {

            for (int i = 0; i < entries; i++) {

                assertTrue(iterator.hasNext());
                IndexEntry poll = iterator.next();
                assertNotNull(poll);
                assertEquals(i, poll.version);
            }

        }
    }

    @Test
    public void next_returns_data_from_disk_and_memory_if_available() throws IOException {
        int entries = 10;
        long stream = 123L;

        for (int i = 0; i < entries; i++) {
            tableIndex.add(stream, i, 0);
        }
        tableIndex.flush();

        for (int i = entries; i < entries * 2; i++) {
            tableIndex.add(stream, i, 0);
        }

        try (IndexIterator iterator = tableIndex.indexedIterator(Checkpoint.of(stream))) {

            for (int i = 0; i < entries * 2; i++) {
                IndexEntry poll = iterator.next();
                assertNotNull(poll);
                assertEquals(i, poll.version);
            }
        }
    }

    @Test
    public void next_returns_null_if_no_inMemory_stream_matches() throws IOException {
        int entries = 10;
        long stream = 123L;
        long someOtherStream = 456L;

        for (int i = 0; i < entries; i++) {
            tableIndex.add(stream, i, 0);
        }

        try (IndexIterator iterator = tableIndex.indexedIterator(Checkpoint.of(someOtherStream))) {
            for (int i = 0; i < entries; i++) {
                IndexEntry poll = iterator.next();
                assertNull(poll);
            }
        }
    }

    @Test
    public void next_returns_null_if_no_disk_stream_matches() throws IOException {
        int entries = 10;
        long stream = 123L;
        long someOtherStream = 456L;

        for (int i = 0; i < entries; i++) {
            tableIndex.add(stream, i, 0);
        }

        tableIndex.flush();

        try (IndexIterator iterator = tableIndex.indexedIterator(Checkpoint.of(someOtherStream))) {
            for (int i = 0; i < entries; i++) {
                IndexEntry poll = iterator.next();
                assertNull(poll);
            }
        }
    }

    @Test
    public void concurrent_write_and_iterator_returns_data_in_sequence() throws Exception {
        int totalEntries = 2500000;

        long stream = 123L;
        Thread writeThread = new Thread(() -> {
            for (int i = 0; i < totalEntries; i++) {
                tableIndex.add(stream, i, 0);
            }
            System.out.println("COMPLETED WRITE");
        });
        writeThread.start();

        try (IndexIterator reader = tableIndex.indexedIterator(Checkpoint.of(stream))) {
            for (int i = 0; i < totalEntries; i++) {
                while (!reader.hasNext()) {
                    Thread.sleep(200);
                }
                IndexEntry entry = reader.next();
                if (i % 100000 == 0) {
                    System.out.println("Read " + i);
                }
                assertNotNull("Failed on " + i + ": " + entry, entry);
                assertEquals("Failed on " + i + ": " + entry, i, entry.version);
            }
            System.out.println("COMPLETED READ");
        }

        writeThread.join();
    }

    @Test
    public void concurrent_write_and_iterator_returns_data_in_sequence_from_multiple_streams() throws Exception {
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
        try (IndexIterator iterator = tableIndex.indexedIterator(Checkpoint.of(streams))) {
            for (int i = 0; i < numVersion * numStreams; i++) {
                Iterators.await(iterator, 500, 10000);
                IndexEntry entry = iterator.next();
                if (i % 100000 == 0) {
                    System.out.println("Polled " + i);
                }
            }
            System.out.println("COMPLETED READ");
        }

        writeThread.join();
    }

    @Test
    public void mem_disk_transition_returns_correct_index_order() throws IOException {
        long stream1 = 123L;
        long stream2 = 456L;

        tableIndex.add(stream2, 0, 0);
        tableIndex.flush();
        tableIndex.add(stream2, 1, 0);

        try (IndexIterator iterator = tableIndex.indexedIterator(Checkpoint.of(Set.of(stream1, stream2)))) {
            IndexEntry polled = iterator.next();
            assertEquals(stream2, polled.stream);
            assertEquals(0, polled.version);

            polled = iterator.next();
            assertEquals(stream2, polled.stream);
            assertEquals(1, polled.version);
        }
    }

    @Test
    public void disk_switches_to_mem_iterator() {

        long stream = 456L;

        tableIndex.add(stream, 0, 0);
        tableIndex.add(stream, 1, 0);

        IndexIterator iterator = tableIndex.indexedIterator(Checkpoint.of(stream));

        IndexEntry polled = iterator.next();
        assertEquals(stream, polled.stream);
        assertEquals(0, polled.version);

        tableIndex.flush();

        polled = iterator.next();
        assertEquals(stream, polled.stream);
        assertEquals(1, polled.version);
    }

    @Test
    public void mem_switches_to_disk_iterator() throws IOException {

        long stream = 456L;

        tableIndex.add(stream, 0, 0);

        try (IndexIterator iterator = tableIndex.indexedIterator(Checkpoint.of(stream))) {
            IndexEntry polled = iterator.next();
            assertEquals(stream, polled.stream);
            assertEquals(0, polled.version);

            tableIndex.add(stream, 1, 0);
            tableIndex.flush();

            polled = iterator.next();
            assertEquals(stream, polled.stream);
            assertEquals(1, polled.version);
        }
    }

    @Test
    public void iterator_returns_the_processed_stream_versions() throws IOException {
        int versions = 10;
        long stream = 456L;

        for (int version = 0; version < versions; version++) {
            tableIndex.add(stream, version, 0);
        }

        try (IndexIterator iterator = tableIndex.indexedIterator(Checkpoint.of(stream))) {
            IndexEntry poll = iterator.next();
            assertEquals(0, poll.version);

            Map<Long, Integer> processed = iterator.processed();
            assertEquals(Integer.valueOf(0), processed.get(stream));
        }
    }

    @Test
    public void iterator_starts_from_the_provided_stream_versions() throws IOException {
        int versions = 10;
        long stream = 456L;

        for (int version = 0; version < versions; version++) {
            tableIndex.add(stream, version, 0);
        }

        Checkpoint processed;
        try (IndexIterator iterator = tableIndex.indexedIterator(Checkpoint.of(stream))) {
            IndexEntry entry = iterator.next();
            assertEquals(0, entry.version);

            processed = iterator.processed();
            assertEquals(Integer.valueOf(0), processed.get(stream));
        }

        try (IndexIterator iterator = tableIndex.indexedIterator(processed)) {
            IndexEntry entry = iterator.next();
            assertEquals(1, entry.version);
        }
    }

    @Test
    public void multiple_streams_return_ordered_when_parameter_is_true() throws IOException {
        int versions = 100;
        long streams = 1000;

        long pos = 0;
        for (long stream = 0; stream < streams; stream++) {
            for (int version = 0; version < versions; version++) {
                tableIndex.add(stream, version, pos++);
            }
        }

        long lastPos = -1;
        try (IndexIterator iterator = tableIndex.indexedIterator(Checkpoint.of(Set.of(200L, 1L, 500L)), true)) {
            while (iterator.hasNext()) {
                IndexEntry ie = iterator.next();
                assertTrue(ie.position > lastPos);
                lastPos = ie.position;
            }
        }
    }

    @Test
    public void indexedIterator_finds_all_entries() throws IOException {

        long[] streams = new long[]{12, -3, 10, 3000, -100, -300, 0, -20, 60};
        int versions = 999999;

        for (long stream : streams) {
            for (int version = 0; version < versions; version++) {
                tableIndex.add(stream, version, 0);
            }
        }

        for (long stream : streams) {
            try (IndexIterator iterator = tableIndex.indexedIterator(Checkpoint.of(stream))) {
                for (int version = 0; version < versions; version++) {
                    assertTrue("Failed on stream " + stream + " version " + version, iterator.hasNext());

                    IndexEntry next = iterator.next();
                    assertEquals("Failed on stream " + stream + " version " + version, stream, next.stream);
                    assertEquals("Failed on stream " + stream + " version " + version, version, next.version);
                }
            }
        }
    }

    @Test
    public void indexedIterator_finds_all_entries_with_multiple_streams() throws IOException {

        Set<Long> streams = Set.of(12L, -3L, 10L, 3000L, -100L, -300L, 0L, -20L, 60L);
        int versions = 499999;

        for (long stream : streams) {
            for (int version = 0; version < versions; version++) {
                tableIndex.add(stream, version, 0);
            }
        }

        final Map<Long, Integer> tracker = new HashMap<>();
        for (Long stream : streams) {
            tracker.put(stream, IndexEntry.NO_VERSION);
        }

        try (IndexIterator iterator = tableIndex.indexedIterator(Checkpoint.of(streams))) {
            while (iterator.hasNext()) {
                IndexEntry next = iterator.next();
                tracker.put(next.stream, next.version);
            }
        }

        for (Map.Entry<Long, Integer> kv : tracker.entrySet()) {
            Long stream = kv.getKey();
            Integer lastVersion = kv.getValue();

            assertEquals("Failed total read stream items of stream " + stream, Integer.valueOf(versions - 1), lastVersion);
        }
    }


}