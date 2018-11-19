package io.joshworks.eventry.index;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.testutils.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
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

        LogIterator<IndexEntry> it = tableIndex.indexedIterator(stream);

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

        Stream<IndexEntry> dataStream = tableIndex.indexedIterator(stream).stream();

        assertEquals(2, dataStream.count());
    }

    @Test
    public void range_stream_with_range_returns_data_from_disk_and_memory() {
        long stream = 1;

        tableIndex.add(stream, 0, 0);
        tableIndex.flush();
        tableIndex.add(stream, 1, 0);

        Stream<IndexEntry> dataStream = tableIndex.indexedIterator(stream).stream();
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

        LogIterator<IndexEntry> it = tableIndex.indexedIterator(stream);

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

        Stream<IndexEntry> dataStream = tableIndex.indexedIterator(stream).stream();
        assertEquals(size, dataStream.count());
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
    public void hasNext_returns_false_when_no_data_is_avaialble() {
        TableIndex.IndexIterator iterator = tableIndex.indexedIterator(1);
        assertFalse(iterator.hasNext());
    }

    @Test
    public void hasNext_returns_true_when_data_is_avaialble() {
        tableIndex.add(1, 0, 0);
        TableIndex.IndexIterator iterator = tableIndex.indexedIterator(1);
        assertTrue(iterator.hasNext());
    }

    @Test
    public void next_returns_data_from_memory() {
        int entries = 500;
        long stream = 123L;
        for (int i = 0; i < entries; i++) {
            tableIndex.add(stream, i, 0);
        }

        try (TableIndex.IndexIterator iterator = tableIndex.indexedIterator(stream)) {

            for (int i = 0; i < entries; i++) {
                IndexEntry poll = iterator.next();
                assertNotNull(poll);
                assertEquals(i, poll.version);
            }

        }
    }

    @Test
    public void next_returns_data_from_multiple_streams() {
        int entries = 1500000;
        Set<Long> streams = new HashSet<>();
        for (long i = 0; i < entries; i++) {
            streams.add(i);
            tableIndex.add(i, 0, 0);
        }

        try (TableIndex.IndexIterator iterator = tableIndex.indexedIterator(streams)) {

            for (int i = 0; i < entries; i++) {
                IndexEntry poll = iterator.next();
                assertNotNull(poll);
                assertTrue(streams.contains(poll.stream));
                assertEquals(0, poll.version);
            }
        }
    }

    @Test
    public void next_returns_data_from_multiple_streams_in_order_per_stream() {
        int streams = 10000;
        int versions = 100;

        Map<Long, Integer> versionTracker = new HashMap<>();
        for (long stream = 0; stream < streams; stream++) {
            versionTracker.put(stream, 0);
            for (int version = 0; version < versions; version++) {
                tableIndex.add(stream, version, 0);
            }
        }

        try (TableIndex.IndexIterator iterator = tableIndex.indexedIterator(versionTracker.keySet())) {

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
    public void next_returns_data_from_disk() {
        int entries = 500;
        long stream = 123L;
        for (int i = 0; i < entries; i++) {
            tableIndex.add(stream, i, 0);
        }

        tableIndex.flush();

        try (TableIndex.IndexIterator iterator = tableIndex.indexedIterator(stream)) {

            for (int i = 0; i < entries; i++) {

                assertTrue(iterator.hasNext());
                IndexEntry poll = iterator.next();
                assertNotNull(poll);
                assertEquals(i, poll.version);
            }

        }
    }

    @Test
    public void next_returns_data_from_disk_and_memory_if_available() {
        int entries = 10;
        long stream = 123L;

        for (int i = 0; i < entries; i++) {
            tableIndex.add(stream, i, 0);
        }
        tableIndex.flush();

        for (int i = entries; i < entries * 2; i++) {
            tableIndex.add(stream, i, 0);
        }

        try (TableIndex.IndexIterator iterator = tableIndex.indexedIterator(stream)) {

            for (int i = 0; i < entries * 2; i++) {
                IndexEntry poll = iterator.next();
                assertNotNull(poll);
                assertEquals(i, poll.version);
            }
        }
    }

    @Test
    public void next_returns_null_if_no_inmemory_stream_matches() {
        int entries = 10;
        long stream = 123L;
        long someOtherStream = 456L;

        for (int i = 0; i < entries; i++) {
            tableIndex.add(stream, i, 0);
        }

        try (TableIndex.IndexIterator iterator = tableIndex.indexedIterator(someOtherStream)) {
            for (int i = 0; i < entries; i++) {
                IndexEntry poll = iterator.next();
                assertNull(poll);
            }
        }
    }

    @Test
    public void next_returns_null_if_no_disk_stream_matches() {
        int entries = 10;
        long stream = 123L;
        long someOtherStream = 456L;

        for (int i = 0; i < entries; i++) {
            tableIndex.add(stream, i, 0);
        }

        tableIndex.flush();

        try (TableIndex.IndexIterator iterator = tableIndex.indexedIterator(someOtherStream)) {
            for (int i = 0; i < entries; i++) {
                IndexEntry poll = iterator.next();
                assertNull(poll);
            }
        }
    }


    @Test
    public void concurrent_write_and_iterator_returns_data_in_sequence() throws InterruptedException {
        int totalEntries = 2500000;

        long stream = 123L;
        Thread writeThread = new Thread(() -> {
            for (int i = 0; i < totalEntries; i++) {
                tableIndex.add(stream, i, 0);
            }
            System.out.println("COMPLETED WRITE");
        });
        writeThread.start();

        try (TableIndex.IndexIterator reader = tableIndex.indexedIterator(stream)) {
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
    public void concurrent_write_and_iterator_returns_data_in_sequence_from_multiple_streams() throws InterruptedException {
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
        try (TableIndex.IndexIterator iterator = tableIndex.indexedIterator(streams)) {
            for (int i = 0; i < numVersion * numStreams; i++) {
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
    public void mem_disk_transition_returns_correct_index_order() {

        long stream1 = 123L;
        long stream2 = 456L;

        tableIndex.add(stream2, 0, 0);
        tableIndex.flush();
        tableIndex.add(stream2, 1, 0);

        TableIndex.IndexIterator iterator = tableIndex.indexedIterator(Set.of(stream1, stream2));

        IndexEntry polled = iterator.next();
        assertEquals(stream2, polled.stream);
        assertEquals(0, polled.version);

        polled = iterator.next();
        assertEquals(stream2, polled.stream);
        assertEquals(1, polled.version);
    }

    @Test
    public void disk_switches_to_mem_iterator() {

        long stream = 456L;

        tableIndex.add(stream, 0, 0);
        tableIndex.add(stream, 1, 0);

        TableIndex.IndexIterator iterator = tableIndex.indexedIterator(stream);

        IndexEntry polled = iterator.next();
        assertEquals(stream, polled.stream);
        assertEquals(0, polled.version);

        tableIndex.flush();

        polled = iterator.next();
        assertEquals(stream, polled.stream);
        assertEquals(1, polled.version);
    }

    @Test
    public void mem_switches_to_disk_iterator() {

        long stream = 456L;

        tableIndex.add(stream, 0, 0);

        TableIndex.IndexIterator iterator = tableIndex.indexedIterator(Set.of(stream));

        IndexEntry polled = iterator.next();
        assertEquals(stream, polled.stream);
        assertEquals(0, polled.version);

        tableIndex.add(stream, 1, 0);
        tableIndex.flush();

        polled = iterator.next();
        assertEquals(stream, polled.stream);
        assertEquals(1, polled.version);
    }

    @Test
    public void iterator_returns_the_processed_stream_versions() {
        int versions = 10;
        long stream = 456L;

        for (int version = 0; version < versions; version++) {
            tableIndex.add(stream, version, 0);
        }

        TableIndex.IndexIterator iterator = tableIndex.indexedIterator(stream);
        IndexEntry poll = iterator.next();
        assertEquals(0, poll.version);

        Map<Long, Integer> processed = iterator.processed();
        assertEquals(Integer.valueOf(0), processed.get(stream));

    }

    @Test
    public void iterator_starts_from_the_provided_stream_versions() {
        int versions = 10;
        long stream = 456L;

        for (int version = 0; version < versions; version++) {
            tableIndex.add(stream, version, 0);
        }

        TableIndex.IndexIterator iterator = tableIndex.indexedIterator(stream);
        IndexEntry entry = iterator.next();
        assertEquals(0, entry.version);

        Map<Long, Integer> processed = iterator.processed();
        assertEquals(Integer.valueOf(0), processed.get(stream));

        iterator.close();

        iterator = tableIndex.indexedIterator(processed);
        entry = iterator.next();
        assertEquals(1, entry.version);
    }

    @Test
    public void indexedIterator_finds_all_entries() {

        long[] streams = new long[]{12, -3, 10, 3000, -100, -300, 0, -20, 60};
        int versions = 999999;

        for (long stream : streams) {
            for (int version = 0; version < versions; version++) {
                tableIndex.add(stream, version, 0);
            }
        }

        for (long stream : streams) {
            try (TableIndex.IndexIterator iterator = tableIndex.indexedIterator(stream)) {
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
    public void indexedIterator_finds_all_entries_with_multiple_streams() {

        Set<Long> streams = Set.of(12L, -3L, 10L, 3000L, -100L, -300L, 0L, -20L, 60L);
        int versions = 999999;

        for (long stream : streams) {
            for (int version = 0; version < versions; version++) {
                tableIndex.add(stream, version, 0);
            }
        }

        final Map<Long, Integer> tracker = new HashMap<>();
        for (Long stream : streams) {
            tracker.put(stream, IndexEntry.NO_VERSION);
        }

        try (TableIndex.IndexIterator iterator = tableIndex.indexedIterator(streams)) {
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