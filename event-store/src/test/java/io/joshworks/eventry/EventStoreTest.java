package io.joshworks.eventry;

import io.joshworks.eventry.data.SystemStreams;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.testutils.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class EventStoreTest {

    private File directory;
    private IEventStore store;

    @Before
    public void setUp() {
        directory = FileUtils.testFolder();
        store = EventStore.open(directory);
    }

    @After
    public void tearDown() {
        store.close();
        FileUtils.tryDelete(directory);
    }

    @Test
    public void system_events_are_loaded_on_reopen() {

        store.close();
        store = EventStore.open(directory);

        EventRecord record = store.get(StreamName.of(SystemStreams.STREAMS, 0));
        assertNotNull(record);
        assertEquals(0, record.version);
        assertEquals(SystemStreams.STREAMS, record.stream);
    }

    @Test
    public void fromStream_returns_all_items_when_store_is_reopened() {

        //given
        int size = 10000;
        String streamPrefix = "test-stream-";
        for (int i = 0; i < size; i++) {
            store.append(EventRecord.create(streamPrefix + i, "" + i, "body-" + i));
        }

        store.close();

        try (IEventStore store = EventStore.open(directory)) {
            for (int i = 0; i < size; i++) {
                Stream<EventRecord> events = store.fromStream(StreamName.parse(streamPrefix + i)).stream();
                assertEquals("Failed on iteration " + i, 1, events.count());
            }
        }
    }

    @Test
    public void index_is_loaded_with_correct_stream_version_order() {
        String stream = "stream-a";
        store.append(EventRecord.create(stream, "type", "body"));
        store.append(EventRecord.create(stream, "type", "body"));
        store.append(EventRecord.create(stream, "type", "body"));

        store.close();

        store = EventStore.open(directory);

        LogIterator<EventRecord> iter = store.fromStream(StreamName.parse(stream));

        EventRecord event = iter.next();
        assertEquals(stream, event.stream);
        assertEquals(0, event.version);

        event = iter.next();
        assertEquals(stream, event.stream);
        assertEquals(1, event.version);

        event = iter.next();
        assertEquals(stream, event.stream);
        assertEquals(2, event.version);
    }

    @Test
    public void fromStream_returns_data_within_maxCount() {
        //given

        String stream = "test-stream";
        int maxCount = 10;
        int numVersions = 50;
        store.createStream(stream, maxCount, -1);

        for (int version = 0; version < numVersions; version++) {
            store.append(EventRecord.create(stream, "type", "body-" + stream));
        }

        Iterator<EventRecord> eventStream = store.fromStream(StreamName.parse(stream));

        int eventCounter = 0;
        while (eventStream.hasNext()) {
            EventRecord event = eventStream.next();
            assertTrue(event.version >= numVersions - maxCount);
            eventCounter++;
        }

        assertEquals(maxCount, eventCounter);
    }

    @Test
    public void truncated_events_are_not_returned() {
        //given

        String stream = "test-stream";
        int versions = 100;

        for (int version = 0; version < versions; version++) {
            store.append(EventRecord.create(stream, "type", "body-" + stream));
        }

        store.truncate(stream, 60);

        long count = store.fromStream(StreamName.parse(stream)).stream().count();
        assertEquals(40, count);

        Iterator<EventRecord> eventStream = store.fromStream(StreamName.parse(stream));

        int expectedVersion = 60;
        while (eventStream.hasNext()) {
            EventRecord event = eventStream.next();
            assertEquals(expectedVersion, event.version);
            expectedVersion++;
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void stream_name_cannot_start_with_system_reserved_prefix() {
        store.append(EventRecord.create(StreamName.SYSTEM_PREFIX + "stream", "a", "asa"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void event_type_is_mandatory() {
        store.append(EventRecord.create("stream", null, "asa"));
    }


    @Test
    public void fromStream_returns_data_within_maxAge() throws InterruptedException {
        //given

        String stream = "test-stream";
        int maxAgeSeconds = 5;
        int numVersions = 50;
        store.createStream(stream, -1, maxAgeSeconds);

        for (int version = 0; version < numVersions; version++) {
            store.append(EventRecord.create(stream, "type", "body-" + stream));
        }

        long count = store.fromStream(StreamName.parse(stream)).stream().count();
        assertEquals("MAY FAIL DUE TO TIMING", numVersions, count);

        Thread.sleep(maxAgeSeconds * 1000);

        count = store.fromStream(StreamName.parse(stream)).stream().count();
        assertEquals(numVersions, count);
    }

    @Test
    public void get_returns_resolved_event() {
        String linkToStream = "link-to-stream";
        EventRecord original = store.append(EventRecord.create("original", "a", Map.of()));
        EventRecord linkTo = store.linkTo(linkToStream, original);

        EventRecord found = store.get(StreamName.from(linkTo));
        assertEquals(original, found);
    }

    @Test
    public void fromStream_returns_items_after_truncated_version() {

        int size = 1000;
        int truncateFrom = 399;
        String stream = "stream-123";
        for (int i = 0; i < size; i++) {
            store.append(EventRecord.create(stream, "test", "body"));
        }

        store.truncate(stream, truncateFrom);

        List<EventRecord> events = store.fromStream(StreamName.parse(stream)).stream().collect(Collectors.toList());
        assertEquals(600, events.size());
        assertEquals(400, events.get(0).version);
    }

    @Test
    public void append() {
        fail("IMPLEMENT ME");
    }


    @Test
    public void compact() {
        fail("IMPLEMENT ME");
    }

    @Test
    public void createStream() {
        fail("IMPLEMENT ME");
    }

    @Test
    public void createStream1() {
        fail("IMPLEMENT ME");
    }

    @Test
    public void createStream2() {
        fail("IMPLEMENT ME");
    }

    @Test
    public void streamsMetadata() {
        fail("IMPLEMENT ME");
    }

    @Test
    public void streamMetadata() {
        fail("IMPLEMENT ME");
    }

    @Test
    public void truncate() {
        fail("IMPLEMENT ME");
    }

    @Test
    public void fromStream() {
        fail("IMPLEMENT ME");
    }

    @Test
    public void fromStreams() {
        fail("IMPLEMENT ME");
    }


    @Test
    public void version() {
        fail("IMPLEMENT ME");
    }

    @Test
    public void fromAll() {
        fail("IMPLEMENT ME");
    }

    @Test
    public void fromAll1() {
        fail("IMPLEMENT ME");
    }

    @Test
    public void get() {
        fail("IMPLEMENT ME");
    }

    @Test
    public void close() {
        fail("IMPLEMENT ME");
    }
}