package io.joshworks.eventry.index.disk;

import io.joshworks.eventry.index.IndexEntry;
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
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class IndexAppenderTest {

    private IndexAppender appender;
    private File location;

    @Before
    public void setUp() {
        location = FileUtils.testFolder();
        appender = new IndexAppender(location, (int) Size.MB.of(10), 10000, true);
    }

    @After
    public void tearDown() {
        appender.close();
        FileUtils.tryDelete(location);
    }

    @Test
    public void entry_count() {

        appender.append(IndexEntry.of(1, 0, 0));
        appender.append(IndexEntry.of(2, 0, 0));
        appender.append(IndexEntry.of(3, 0, 0));

        long entries = appender.entries();
        assertEquals(3, entries);
    }

    @Test
    public void entry_count_after_flush() {

        appender.append(IndexEntry.of(1, 0, 0));
        appender.append(IndexEntry.of(2, 0, 0));
        appender.append(IndexEntry.of(3, 0, 0));

        appender.flush();

        long entries = appender.entries();
        assertEquals(3, entries);
    }

    @Test
    public void entry_count_after_close() {

        appender.append(IndexEntry.of(1, 0, 0));
        appender.append(IndexEntry.of(2, 0, 0));
        appender.append(IndexEntry.of(3, 0, 0));

        appender.close();

        appender = new IndexAppender(location, (int) Size.MB.of(10), 10000, true);
        long entries = appender.entries();

        //FIXME BLOCK SEGMENTS MUST PARSE ALL BLOCKS ON OPEN TO COMPUTE THE ENTRIES, IF ITS LOG_HEAD
        assertEquals(3, entries);
    }

    @Test
    public void all_entries_are_returned_from_multiple_segments() {

        int entriesPerSegment = 10;
        int numSegments = 10;

        int stream = 0;
        for (int i = 0; i < entriesPerSegment; i++) {
            for (int x = 0; x < numSegments; x++) {
                appender.append(IndexEntry.of(stream++, 0, 0));
            }
            appender.roll();
        }
        appender.flush();

        long entries = appender.entries();
        assertEquals(entriesPerSegment * numSegments, entries);

        try (Stream<IndexEntry> items = appender.indexStream(Direction.FORWARD)) {
            assertEquals(entriesPerSegment * numSegments, items.count());

        }
    }

    @Test
    public void entries_are_returned_in_order_from_multiple_segments() throws IOException {

        int entriesPerSegment = 10;
        int numSegments = 10;

        int version = 0;
        for (int i = 0; i < entriesPerSegment; i++) {
            for (int x = 0; x < numSegments; x++) {
                appender.append(IndexEntry.of(0, version++, 0));
            }
            appender.roll();
        }
        appender.flush();

        int found = 0;
        int expectedVersion = Range.START_VERSION;
        try (LogIterator<IndexEntry> iterator = appender.indexIterator(Direction.FORWARD)) {
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
            appender.append(IndexEntry.of(i, 1, 0));
        }
        appender.flush();
        System.out.println("WRITE " + (System.currentTimeMillis() - start));


        start = System.currentTimeMillis();
        int count = 0;
        IndexEntry last = null;
        Iterator<IndexEntry> iterator = appender.indexIterator(Direction.FORWARD);
        while (iterator.hasNext()) {
            IndexEntry next = iterator.next();
            last = next;
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
            appender.append(IndexEntry.of(i, 1, 0));
            if (i % itemsPerSegment == 0) {
                appender.roll();
            }
        }
        appender.flush();

        long start = System.currentTimeMillis();
        for (int i = 0; i < streams; i++) {

            int version = appender.version(i);
            if (i % 100000 == 0) {
                System.out.println((System.currentTimeMillis() - start));
                start = System.currentTimeMillis();
            }
            assertEquals("Failed on iteration " + i, 1, version);

        }
    }

    @Test
    public void version_is_minus_one_for_non_existing_stream() {
        int version = appender.version(1234);
        assertEquals(IndexEntry.NO_VERSION, version);
    }
}