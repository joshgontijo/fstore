package io.joshworks.eventry.index.disk;

import io.joshworks.eventry.index.IndexEntry;
import io.joshworks.eventry.index.Range;
import io.joshworks.fstore.codec.snappy.SnappyCodec;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.util.Memory;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.segment.WriteMode;
import io.joshworks.fstore.testutils.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static io.joshworks.eventry.log.EventRecord.NO_VERSION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class IndexSegmentTest {

    private File segmentFile;
    private File indexDir;

    private static final int MAX_ENTRY_SIZE = 1024 * 1024 * 5;
    private static final double CHECKSUM_PROB = 1;
    private static final int READ_PAGE_SIZE = Memory.PAGE_SIZE;

    private IndexSegment segment;
    private static final int NUMBER_OF_ELEMENTS = 1000000; //bloom filter

    @Before
    public void setUp() {
        indexDir = FileUtils.testFolder();
        segmentFile = new File(indexDir, "test-index");
        segment = open(segmentFile);
    }

    @After
    public void tearDown() throws Exception {
        IOUtils.closeQuietly(segment);
        Files.delete(segmentFile.toPath());
    }

    private IndexSegment open(File location) {
        return new IndexSegment(
                location,
                StorageMode.RAF,
                Size.MB.of(100),
                new BufferPool(),
                WriteMode.LOG_HEAD,
                indexDir,
                new SnappyCodec(),
                CHECKSUM_PROB,
                READ_PAGE_SIZE,
                NUMBER_OF_ELEMENTS);
    }

    @Test
    public void range_query_returns_the_correct_midpoint() {

        //given
        IndexSegment diskIndex = indexWithStreamRanging(0, 1000000);

        //when
        long size = diskIndex.indexedIterator(Direction.FORWARD, Range.anyOf(0)).stream().count();

        //then
        assertEquals(1L, size);
    }

    @Test
    public void loaded_segmentIndex_has_the_same_10_midpoints() {
        //given
        IndexSegment diskIndex = indexWithStreamRanging(1, 10);

        //when
        diskIndex.close();
        try (IndexSegment loaded = open(segmentFile)) {

            //then
            assertEquals(diskIndex.midpoints.size(), loaded.midpoints.size());

            assertNotNull(diskIndex.midpoints.first());
            assertNotNull(diskIndex.midpoints.last());

            assertEquals(diskIndex.midpoints.first(), loaded.midpoints.first());
            assertEquals(diskIndex.midpoints.last(), loaded.midpoints.last());
        }
    }

    @Test
    public void loaded_segmentIndex_has_the_same_1000000_midpoints() {
        //given
        IndexSegment diskIndex = indexWithStreamRanging(1, 1000000);

        //when
        diskIndex.close();
        try (IndexSegment loaded = open(segmentFile)) {
            //then
            assertEquals(diskIndex.midpoints.size(), loaded.midpoints.size());
            assertEquals(diskIndex.midpoints.first(), loaded.midpoints.first());
            assertEquals(diskIndex.midpoints.last(), loaded.midpoints.last());
        }
    }

    @Test
    public void loaded_segmentIndex_has_the_same_filter_items() {
        //given

        IndexSegment diskIndex = indexWithStreamRanging(1, 1000000);

        //when
        assertFalse(diskIndex.filter.contains(0L));
        assertFalse(diskIndex.filter.contains(1000001L));
        diskIndex.close();
        try (IndexSegment loaded = open(segmentFile)) {
            //then

            assertEquals(diskIndex.filter, loaded.filter);

            assertTrue(loaded.filter.contains(1L));
            assertFalse(loaded.filter.contains(-1L));

            loaded.append(IndexEntry.of(999999999L, 1, 0));
            assertTrue(loaded.filter.contains(999999999L));

        }
    }

    @Test
    public void closing_does_not_flush_entries_to_disk() {
        //given
        segment.append(IndexEntry.of(1L, 1, 0));

        //when
        segment.close();
        try (IndexSegment opened = open(segmentFile)) {
            //then
            long items = opened.iterator(Direction.FORWARD).stream().count();
            assertEquals(0, items);
        }
    }

    @Test
    public void reopen_loads_all_four_entries() {
        //given
        segment.append(IndexEntry.of(1L, 1, 0));
        segment.append(IndexEntry.of(1L, 2, 0));
        segment.append(IndexEntry.of(1L, 3, 0));
        segment.append(IndexEntry.of(1L, 4, 0));

        //when
        segment.flush();
        segment.close();
        try (IndexSegment opened = open(segmentFile)) {

            //then
            long items = opened.iterator(Direction.FORWARD).stream().count();
            assertEquals(4, items);
        }
    }

    @Test
    public void reopened_segment_returns_correct_data() {

        //given
        IndexEntry e1 = IndexEntry.of(1L, 1, 0);
        IndexEntry e2 = IndexEntry.of(1L, 2, 0);
        IndexEntry e3 = IndexEntry.of(1L, 3, 0);
        IndexEntry e4 = IndexEntry.of(1L, 4, 0);

        segment.append(e1);
        segment.append(e2);
        segment.append(e3);
        segment.append(e4);

        segment.roll(1);

        Optional<IndexEntry> ie = segment.get(1L, 1);
        assertTrue(ie.isPresent());
        assertEquals(e1, ie.get());

        //when
        segment.close();
        try (IndexSegment opened = open(segmentFile)) {
            //then
            long items = opened.iterator(Direction.FORWARD).stream().count();
            assertEquals(4, items);

            Optional<IndexEntry> found = opened.get(1L, 1);
            assertTrue(found.isPresent());
            assertEquals(e1, found.get());

            Stream<IndexEntry> stream = opened.indexedIterator(Direction.FORWARD, Range.anyOf(1L)).stream();
            assertEquals(4, stream.count());
        }
    }

    @Test
    public void range_query_returns_the_correct_midpoint_for_negative_hash() {
        //given
        IndexSegment diskIndex = indexWithStreamRanging(-5, 0);

        //when
        long size = diskIndex.indexedIterator(Direction.FORWARD, Range.anyOf(-5)).stream().count();

        //then
        assertEquals(1, size);
    }

    @Test
    public void range_query_returns_all_version_of_stream() {

        //given
        long stream = 0;
        int startVersion = 0;
        int endVersion = 9;
        IndexSegment diskIndex = indexWithSameStreamWithVersionRanging(stream, startVersion, endVersion);

        //when
        long size = diskIndex.indexedIterator(Direction.FORWARD, Range.anyOf(stream)).stream().count();

        //then
        assertEquals(10, size);
    }

    @Test
    public void lastVersion_with_index_full_of_the_same_stream() {

        //given
        long stream = 123L;
        int endVersion = 499;
        IndexSegment diskIndex = indexWithSameStreamWithVersionRanging(stream, 0, endVersion);

        //when
        int version = diskIndex.lastVersionOf(stream);

        //then
        assertEquals(endVersion, version);
    }

    @Test
    public void firstVersion_with_index_full_of_the_same_stream() {

        //given
        long stream = 123L;
        int firstVersion = 100;
        int latestVersion = 500;
        IndexSegment diskIndex = indexWithSameStreamWithVersionRanging(stream, firstVersion, latestVersion);

        //when
        int version = diskIndex.firstVersionOf(stream);

        //then
        assertEquals(firstVersion, version);
    }

    @Test
    public void firstVersion_with_multiple_streams_with_single_version() {
        //given
        int startStream = 1;
        int endStream = 100000;
        IndexSegment diskIndex = indexWithStreamRanging(startStream, endStream);

        //when
        for (int stream = startStream; stream < endStream; stream++) {
            //when
            int version = diskIndex.firstVersionOf(stream);

            //then
            assertEquals(0, version);
        }
    }

    @Test
    public void firstVersion_with_multiple_streams_with_multiple_versions() {
        //given
        int startStream = 1;
        int endStream = 10000;
        int endVersion = 450;
        IndexSegment diskIndex = indexWithXStreamsWithYEventsEach(endStream, endVersion);

        //when
        for (int stream = startStream; stream < endStream; stream++) {
            //when
            int version = diskIndex.firstVersionOf(stream);

            //then
            assertEquals(0, version);
        }
    }

    @Test
    public void range_with_multiple_streams_with_single_version() {
        //given
        int startStream = 1;
        int endStream = 100000;
        IndexSegment diskIndex = indexWithStreamRanging(startStream, endStream);

        //when
        for (int i = startStream; i < endStream; i++) {
            long size = diskIndex.indexedIterator(Direction.FORWARD, Range.anyOf(i)).stream().count();

            //then
            assertEquals("Failed on position " + i, 1, size);
        }
    }

    @Test
    public void range_with_multiple_streams_with_multiple_versions() {
        //given
        int startStream = 1;
        int endStream = 10000;
        int endVersion = 499;
        IndexSegment diskIndex = indexWithXStreamsWithYEventsEach(endStream, endVersion);

        //when
        for (int i = startStream; i < endStream; i++) {
            long count = diskIndex.indexedIterator(Direction.FORWARD, Range.anyOf(i)).stream().count();

            //then
            assertEquals("Failed on position " + i, endVersion + 1, count);
        }
    }

    @Test
    public void stream_version_with_index_10000_streams() {

        //given
        int startStream = 1;
        int endStream = 10000;
        IndexSegment diskIndex = indexWithStreamRanging(startStream, endStream);

        //when
        for (int i = startStream; i <= endStream; i++) {
            int version = diskIndex.lastVersionOf(i);
            //then
            assertEquals("Failed on iteration " + i, 0, version);
        }
    }

    @Test
    public void stream_version_with_index_100000_streams_and_5_versions_each() {

        //given
        int startStream = 1;
        int endStream = 10000;
        int numVersions = 9;
        IndexSegment diskIndex = indexWithXStreamsWithYEventsEach(endStream, numVersions);

        //when
        for (int i = startStream; i < endStream; i++) {
            int version = diskIndex.lastVersionOf(i);
            //then
            assertEquals("Failed on iteration " + i, numVersions, version);
        }
    }

    @Test
    public void iterator_return_all_entries_with_index_containing_same_stream() {

        long stream = 1;
        int endVersion = 999;
        IndexSegment diskIndex = indexWithSameStreamWithVersionRanging(stream, 0, endVersion);

        for (int i = 0; i < endVersion; i++) {
            //when
            Iterator<IndexEntry> iterator = diskIndex.indexedIterator(Direction.FORWARD, Range.anyOf(stream));

            //then
            assertIteratorHasAllEntries(stream, endVersion, iterator);
        }
    }

    @Test
    public void iterator_return_all_entries_with_index_containing_multiple_streams() {

        int numStreams = 1000;
        IndexSegment diskIndex = indexWithStreamRanging(1, numStreams);

        for (int stream = 1; stream < numStreams; stream++) {
            //when
            Iterator<IndexEntry> iterator = diskIndex.indexedIterator(Direction.FORWARD, Range.anyOf(stream));

            //then
            assertTrue(iterator.hasNext());
            IndexEntry next = iterator.next();

            assertEquals(stream, next.stream);
            assertEquals(0, next.version);
            assertFalse("Failed on " + stream, iterator.hasNext());
        }
    }

    @Test
    public void iterator_with_multiple_events_and_streams() {

        int numStreams = 200;
        int endVersion = 499;

        IndexSegment diskIndex = indexWithXStreamsWithYEventsEach(numStreams, endVersion);

        for (int stream = 0; stream < numStreams; stream++) {
            Iterator<IndexEntry> iterator = diskIndex.indexedIterator(Direction.FORWARD, Range.anyOf(stream));
            assertIteratorHasAllEntries(stream, endVersion, iterator);
        }
    }

    @Test
    public void full_scan_iterator() {

        int numStreams = 200;
        int endVersion = 499;

        IndexSegment diskIndex = indexWithXStreamsWithYEventsEach(numStreams, endVersion);

        Iterator<IndexEntry> iterator = diskIndex.iterator(Direction.FORWARD);

        int count = 0;
        IndexEntry previousEntry = null;
        while (iterator.hasNext()) {
            IndexEntry current = iterator.next();
            if (previousEntry != null) {
                assertTrue(current.greaterThan(previousEntry));
            }
            previousEntry = current;
            count++;
        }

        assertEquals(numStreams * (endVersion + 1), count);
    }

    @Test
    public void when_range_query_gt_index_bounds_return_empty_set() {

        //given
        long stream = 1;
        long streamQuery = 2;

        segment.append(IndexEntry.of(stream, 1, 0));
        segment.append(IndexEntry.of(stream, 2, 0));
        segment.append(IndexEntry.of(stream, 3, 0));
        segment.append(IndexEntry.of(stream, 4, 0));

        //when
        Range range = Range.of(streamQuery, 1);
        Stream<IndexEntry> items = segment.indexedIterator(Direction.FORWARD, range).stream();

        //then
        assertEquals(0, items.count());
    }

    @Test
    public void iterator_return_version_in_increasing_order() {

        long stream = 1;

        segment.append(IndexEntry.of(stream, 1, 0));
        segment.append(IndexEntry.of(stream, 2, 0));
        segment.append(IndexEntry.of(stream, 3, 0));
        segment.append(IndexEntry.of(stream, 4, 0));

        segment.roll(1);

        Range range = Range.of(stream, 1);

        assertEquals(4, segment.indexedIterator(Direction.FORWARD, range).stream().count());

        LogIterator<IndexEntry> iterator = segment.iterator(Direction.FORWARD);

        int lastVersion = 0;
        while (iterator.hasNext()) {
            IndexEntry next = iterator.next();
            assertEquals(lastVersion + 1, next.version);
            lastVersion = next.version;
        }
    }

    @Test
    public void when_range_query_lt_index_bounds_return_empty_set() {

        long stream = 2;
        long streamQuery = 1;


        segment.append(IndexEntry.of(stream, 1, 0));
        segment.append(IndexEntry.of(stream, 2, 0));
        segment.append(IndexEntry.of(stream, 3, 0));
        segment.append(IndexEntry.of(stream, 4, 0));

        segment.roll(1);

        Range range = Range.of(streamQuery, 1);

        assertEquals(0, segment.indexedIterator(Direction.FORWARD, range).stream().count());
    }

    @Test
    public void when_range_query_in_index_bounds_return_all_matches() {

        long stream = 1;

        segment.append(IndexEntry.of(stream, 1, 0));
        segment.append(IndexEntry.of(stream, 2, 0));
        segment.append(IndexEntry.of(stream, 3, 0));
        segment.append(IndexEntry.of(stream, 4, 0));

        segment.roll(1);

        Range range = Range.of(stream, 1, 3);

        Iterator<IndexEntry> it = segment.indexedIterator(Direction.FORWARD, range);

        assertTrue(it.hasNext());
        IndexEntry next = it.next();
        assertEquals(stream, next.stream);
        assertEquals(1, next.version);

        assertTrue(it.hasNext());
        next = it.next();
        assertEquals(stream, next.stream);
        assertEquals(2, next.version);

        assertFalse(it.hasNext());
    }


    @Test
    public void version_of_many_items() {

        //given
        int numStreams = 500000;
        IndexSegment testSegment = indexWithStreamRanging(0, numStreams);

        for (int i = 0; i < numStreams; i++) {
            //when
            int version = testSegment.lastVersionOf(i);

            //then
            assertEquals("Failed on iteration " + i, 0, version);
        }
    }

    @Test
    public void version_of_nonexistent_stream_returns_zero() {

        //given
        int numStreams = 1000;
        int numOfQueries = 100000;
        IndexSegment testSegment = indexWithStreamRanging(0, numStreams);

        for (int i = numStreams + 1; i < numOfQueries; i++) {
            //when
            int version = testSegment.lastVersionOf(i);

            //then
            assertEquals("Failed on iteration " + i, NO_VERSION, version);
        }

    }

    @Test
    public void full_iterator_backwards() {
        //given
        int stream = 123;
        int startVersion = 0;
        int endVersion = 499;
        IndexSegment segment = indexWithSameStreamWithVersionRanging(stream, startVersion, endVersion);

        LogIterator<IndexEntry> iterator = segment.iterator(Direction.BACKWARD);

        int expectedVersion = endVersion;
        int read = 0;
        while (iterator.hasNext()) {
            IndexEntry next = iterator.next();
            assertEquals(expectedVersion, next.version);
            expectedVersion--;
            read++;
        }

        assertEquals(500, read);
    }

    @Test
    public void range_iterator_forward() {
        //given
        int stream = 123;
        int startVersion = 0;
        int endVersion = 499;
        IndexSegment segment = indexWithSameStreamWithVersionRanging(stream, startVersion, endVersion);

        int rangeStart = 100;
        int rangeEnd = 200;

        LogIterator<IndexEntry> iterator = segment.indexedIterator(Direction.FORWARD, Range.of(stream, rangeStart, rangeEnd));

        int expectedVersion = rangeStart;
        int read = 0;
        while (iterator.hasNext()) {
            IndexEntry next = iterator.next();
            assertEquals(expectedVersion, next.version);
            expectedVersion++;
            read++;
        }

        assertEquals(100, read);
    }

    @Test
    public void range_iterator_backward() {
        //given
        int stream = 123;
        int startVersion = 0;
        int endVersion = 499;
        IndexSegment segment = indexWithSameStreamWithVersionRanging(stream, startVersion, endVersion);

        int rangeStart = 100;
        int rangeEnd = 200;

        LogIterator<IndexEntry> iterator = segment.indexedIterator(Direction.BACKWARD, Range.of(stream, rangeStart, rangeEnd));

        int expectedVersion = rangeEnd - 1;
        int read = 0;
        while (iterator.hasNext()) {
            IndexEntry next = iterator.next();
            assertEquals(expectedVersion, next.version);
            expectedVersion--;
            read++;
        }

        assertEquals(100, read);
    }

    @Test
    public void full_range_iterator_backward() {
        //given
        int stream = 123;
        int startVersion = 0;
        int endVersion = 499;
        IndexSegment segment = indexWithSameStreamWithVersionRanging(stream, startVersion, endVersion);

        LogIterator<IndexEntry> iterator = segment.indexedIterator(Direction.BACKWARD, Range.of(stream, startVersion, endVersion + 1));

        int expectedVersion = endVersion;
        int read = 0;
        while (iterator.hasNext()) {
            IndexEntry next = iterator.next();
            assertEquals(expectedVersion, next.version);
            expectedVersion--;
            read++;
        }

        assertEquals(500, read);
    }

    @Test
    public void full_range_iterator_forward() {
        int stream = 123;
        int startVersion = 0;
        int endVersion = 499;
        IndexSegment segment = indexWithSameStreamWithVersionRanging(stream, startVersion, endVersion);

        LogIterator<IndexEntry> iterator = segment.indexedIterator(Direction.FORWARD, Range.of(stream, startVersion, endVersion + 1));

        int expectedVersion = startVersion;
        int read = 0;
        while (iterator.hasNext()) {
            IndexEntry next = iterator.next();
            assertEquals(expectedVersion, next.version);
            expectedVersion++;
            read++;
        }

        assertEquals(500, read);
    }

    @Test
    public void firstVersionOf_returns_NO_VERSION_if_no_matches() {
        int version = segment.firstVersionOf(1);
        assertEquals(NO_VERSION, version);
    }

    @Test
    public void lastVersionOf_returns_NO_VERSION_if_no_matches() {
        int version = segment.lastVersionOf(1);
        assertEquals(NO_VERSION, version);
    }

    @Test
    public void firstVersionOf_returns_NO_VERSION_if_no_matches_with_segment_data() {
        for (int i = 0; i < 10; i++) {
            segment.append(IndexEntry.of(i, 0, 0));
        }
        int version = segment.firstVersionOf(999);
        assertEquals(NO_VERSION, version);
    }

    @Test
    public void full_range_iterator_forward_with_single_entry() {
        int stream = 123;
        segment.append(IndexEntry.of(stream, 0, 0));
        segment.roll(1);

        LogIterator<IndexEntry> iterator = segment.indexedIterator(Direction.FORWARD, Range.anyOf(stream));

        assertTrue(iterator.hasNext());
        assertEquals(0, iterator.next().version);

        assertFalse(iterator.hasNext());
    }

    @Test
    public void readBlockEntries_returns_correct_block_for_all_entries() {
        int streams = 1000;
        int versions = 10;
        IndexSegment segment = indexWithXStreamsWithYEventsEach(streams, versions);

        for (int stream = 0; stream < streams; stream++) {
            for (int version = 0; version < versions; version++) {
                List<IndexEntry> indexEntries = segment.readBlockEntries(stream, version);

                final int theStream = stream;
                final int theVersion = version;
                boolean found = indexEntries.stream().anyMatch(ie -> ie.version == theVersion && ie.stream == theStream);
                assertTrue("Failed on stream " + theStream + ", version " + theVersion, found);
            }
        }
    }

    @Test
    public void readBlockEntries_returns_correct_block_for_sparse_stream() {
        long[] streams = new long[]{-300, -2, 0, 1, 5, 200, 500};

        int versions = 99999;
        for (long stream : streams) {
            for (int version = 0; version < versions; version++) {
                segment.append(IndexEntry.of(stream, version, 0));
            }
        }
        segment.flush();

        for (long stream : streams) {
            for (int version = 0; version < versions; version++) {
                List<IndexEntry> indexEntries = segment.readBlockEntries(stream, version);

                final int theVersion = version;
                boolean found = indexEntries.stream().anyMatch(ie -> ie.version == theVersion && ie.stream == stream);
                assertTrue("Failed on stream " + stream + ", version " + theVersion, found);
            }
        }
    }


    @Test
    public void readBlockEntries_returns_correct_block_for_sparse_stream_1() {
        long[] streams = new long[]{-1, 0};

        int versions = 999999;
        for (long stream : streams) {
            for (int version = 0; version < versions; version++) {
                segment.append(IndexEntry.of(stream, version, 0));
            }
        }
        segment.flush();

        for (long stream : streams) {
            for (int version = 0; version < versions; version++) {
                List<IndexEntry> indexEntries = segment.readBlockEntries(stream, version);

                final int theVersion = version;
                boolean found = indexEntries.stream().anyMatch(ie -> ie.version == theVersion && ie.stream == stream);
                assertTrue("Failed on stream " + stream + ", version " + theVersion, found);
            }
        }
    }

    @Test
    public void get_returns_correct_block_for_all_entries() {
        int streams = 1000;
        int versions = 1000;
        IndexSegment segment = indexWithXStreamsWithYEventsEach(streams, versions);

        for (int stream = 0; stream < streams; stream++) {
            for (int version = 0; version < versions; version++) {
                Optional<IndexEntry> found = segment.get(stream, version);
                assertTrue("Failed on stream " + stream + ", version " + version, found.isPresent());
            }
        }
    }

    @Test
    public void holes() {
        long stream = 1;
        segment.append(IndexEntry.of(stream, 1, 0));
        segment.append(IndexEntry.of(stream, 2, 0));

        segment.append(IndexEntry.of(stream, 1, -1));
        segment.append(IndexEntry.of(stream, 2, -1));
        segment.flush();

        List<IndexEntry> indexEntries = segment.readBlockEntries(stream, 1);

        assertEquals(2, indexEntries.size());

        IndexEntry first = indexEntries.get(0);
        assertEquals(1, first.version);
        assertEquals(-1, first.position);

        IndexEntry second = indexEntries.get(0);
        assertEquals(1, second.version);
        assertEquals(-1, second.position);

    }

    private void assertIteratorHasAllEntries(long stream, int lastEventVersion, Iterator<IndexEntry> iterator) {
        int expectedVersion = 0;
        int count = 0;
        while (iterator.hasNext()) {
            IndexEntry next = iterator.next();

            assertEquals(stream, next.stream);
            assertEquals(expectedVersion, next.version);
            expectedVersion = next.version + 1;
            count++;
        }

        assertEquals(lastEventVersion + 1, count);
    }

    private IndexSegment indexWithStreamRanging(int from, int to) {
        //given
        for (int i = from; i <= to; i++) {
            segment.append(IndexEntry.of(i, 0, 0));
        }
        segment.roll(1);
        return segment;
    }

    private IndexSegment indexWithSameStreamWithVersionRanging(long stream, int from, int to) {
        for (int i = from; i <= to; i++) {
            segment.append(IndexEntry.of(stream, i, 0));
        }
        segment.roll(1);
        return segment;
    }

    private IndexSegment indexWithXStreamsWithYEventsEach(int streamQtd, int endVersion) {
        //given
        for (int stream = 0; stream < streamQtd; stream++) {
            for (int version = 0; version <= endVersion; version++) {
                segment.append(IndexEntry.of(stream, version, 0));
            }
        }
        segment.roll(1);
        return segment;
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}