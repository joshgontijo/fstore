package io.joshworks.eventry.index.disk;

import io.joshworks.eventry.index.IndexEntry;
import io.joshworks.eventry.index.MemIndex;
import io.joshworks.eventry.index.Range;
import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.fstore.codec.snappy.SnappyCodec;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.testutils.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class IndexAppenderTest {

    private IndexAppender appender;
    private File location;
    private MemIndex memIndex;

    @Before
    public void setUp() {
        location = FileUtils.testFolder();
        appender = new IndexAppender(location, e -> emptyStreamMeta(), 10000, new SnappyCodec());
        memIndex = new MemIndex();
    }

    @After
    public void tearDown() {
        appender.close();
        FileUtils.tryDelete(location);
    }

    @Test
    public void entry_count() {

        memIndex.add(IndexEntry.of(1, 0, 0));
        memIndex.add(IndexEntry.of(2, 0, 0));
        memIndex.add(IndexEntry.of(3, 0, 0));

        appender.writeToDisk(memIndex);

        long entries = appender.entries();
        assertEquals(3, entries);
    }

    @Test
    public void entry_count_after_close() {

        memIndex.add(IndexEntry.of(1, 0, 0));
        memIndex.add(IndexEntry.of(2, 0, 0));
        memIndex.add(IndexEntry.of(3, 0, 0));

        appender.writeToDisk(memIndex);
        appender.close();

        appender = new IndexAppender(location, e -> null, 10000, new SnappyCodec());
        long entries = appender.entries();

        assertEquals(3, entries);
    }

    @Test
    public void entries_are_returned_in_order_from_multiple_segments() throws IOException {

        int entriesPerSegment = 10;
        int numSegments = 10;

        int version = 0;
        long stream = 123L;
        for (int i = 0; i < entriesPerSegment; i++) {
            for (int x = 0; x < numSegments; x++) {
                memIndex.add(IndexEntry.of(stream, version++, 0));
            }
            appender.writeToDisk(memIndex);
            memIndex = new MemIndex();
        }

        int found = 0;
        int expectedVersion = Range.START_VERSION;
        try (LogIterator<IndexEntry> iterator = appender.indexedIterator(Direction.FORWARD, Range.anyOf(stream))) {
            while (iterator.hasNext()) {
                IndexEntry next = iterator.next();
                assertEquals(expectedVersion, next.version);
                found++;
                expectedVersion++;
            }
            assertEquals(entriesPerSegment * numSegments, found);
        }
    }

    @Test
    public void writeToDisk_persists_all_memtable_entries() {
        int numEntries = 1000000;
        for (int i = 0; i < numEntries; i++) {
            memIndex.add(IndexEntry.of(i, 1, 0));
        }
        appender.writeToDisk(memIndex);

        for (int i = 0; i < numEntries; i++) {
            Optional<IndexEntry> indexEntry = appender.get(i, 1);
            assertTrue("Failed on " + i, indexEntry.isPresent());
        }

        LogIterator<IndexEntry> iterator = appender.iterator(Direction.FORWARD);
        IndexEntry last = null;
        while (iterator.hasNext()) {
            IndexEntry ie = iterator.next();
            if (last == null) {
                last = ie;
                continue;
            }
            assertEquals(last.stream + 1, ie.stream);
            last = ie;
        }

    }

    @Test
    public void version_with_multiple_segments_returns_correct_version() {

        //given
        int streams = 1000000;
        int numSegments = 2;
        int itemsPerSegment = streams / numSegments;
        for (int i = 0; i < streams; i++) {
            memIndex.add(IndexEntry.of(i, 1, 0));
            if (i % itemsPerSegment == 0) {
                appender.writeToDisk(memIndex);
                memIndex = new MemIndex();
            }
        }

        appender.writeToDisk(memIndex);

        for (int i = 0; i < streams; i++) {
            int version = appender.version(i);
            assertEquals("Failed on iteration " + i, 1, version);

        }
    }

    @Test
    public void version_is_minus_one_for_non_existing_stream() {
        int version = appender.version(1234);
        assertEquals(IndexEntry.NO_VERSION, version);
    }

    private static StreamMetadata emptyStreamMeta() {
        return new StreamMetadata("", 0L, -1L, -1L, -1, -1, new HashMap<>(), new HashMap<>(), 0);
    }

}