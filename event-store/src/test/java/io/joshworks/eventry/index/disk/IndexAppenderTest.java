package io.joshworks.eventry.index.disk;

import io.joshworks.eventry.index.IndexEntry;
import io.joshworks.eventry.index.MemIndex;
import io.joshworks.eventry.index.Range;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.testutils.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;

public class IndexAppenderTest {

    private IndexAppender appender;
    private File location;
    private MemIndex memIndex;

    @Before
    public void setUp() {
        location = FileUtils.testFolder();
        appender = new IndexAppender(location, e -> null, (int) Size.MB.of(10), 10000, true);
        memIndex = new MemIndex();
    }

    @After
    public void tearDown() {
        appender.close();
//        FileUtils.tryDelete(location);
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

        appender = new IndexAppender(location, e -> null, (int) Size.MB.of(10), 10000, true);
        long entries = appender.entries();

        assertEquals(3, entries);
    }


    //FIXME
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
    public void write() {

        long start = System.currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {
            memIndex.add(IndexEntry.of(i, 1, 0));
        }
        appender.writeToDisk(memIndex);
        System.out.println("WRITE " + (System.currentTimeMillis() - start));


        start = System.currentTimeMillis();
        int count = 0;
        IndexEntry last = null;
        Iterator<IndexEntry> iterator = appender.iterator(Direction.FORWARD);
        while (iterator.hasNext()) {
            last = iterator.next();
            count++;
        }

        System.out.println(last);
        System.out.println("READ " + (System.currentTimeMillis() - start));
        assertEquals(1000000, count);

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
}