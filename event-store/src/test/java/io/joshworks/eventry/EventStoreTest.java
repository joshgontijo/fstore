package io.joshworks.eventry;

import io.joshworks.eventry.data.SystemStreams;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.stream.StreamException;
import io.joshworks.eventry.stream.StreamInfo;
import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.testutils.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.joshworks.eventry.log.EventRecord.NO_EXPECTED_VERSION;
import static io.joshworks.eventry.log.EventRecord.NO_VERSION;
import static io.joshworks.eventry.stream.StreamMetadata.NO_MAX_AGE;
import static io.joshworks.eventry.stream.StreamMetadata.NO_MAX_COUNT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
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

        List<EventRecord> stream = store.fromStream(StreamName.of(SystemStreams.STREAMS)).stream().collect(Collectors.toList());
        assertEquals(3, stream.size());
    }

    @Test
    public void fromStream_returns_all_items_when_store_is_reopened() {

        //given
        int size = 10000;
        String streamPrefix = "test-stream-";
        for (int i = 0; i < size; i++) {
            store.append(EventRecord.create(streamPrefix + i, "" + i, Map.of()));
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
        store.append(EventRecord.create(stream, "type", Map.of()));
        store.append(EventRecord.create(stream, "type", Map.of()));
        store.append(EventRecord.create(stream, "type", Map.of()));

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
        store.createStream(stream, maxCount, NO_MAX_AGE);

        for (int version = 0; version < numVersions; version++) {
            store.append(EventRecord.create(stream, "type", Map.of()));
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
            store.append(EventRecord.create(stream, "type", Map.of()));
        }

        store.truncate(stream, 59);

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
        store.append(EventRecord.create(StreamName.SYSTEM_PREFIX + "stream", "a", Map.of()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void event_type_is_mandatory() {
        store.append(EventRecord.create("stream", null, Map.of()));
    }


    @Test
    public void fromStream_returns_data_within_maxAge() throws InterruptedException {
        String stream = "test-stream";
        int maxAgeSeconds = 5;
        int numVersions = 50;
        store.createStream(stream, NO_MAX_COUNT, maxAgeSeconds);

        for (int version = 0; version < numVersions; version++) {
            store.append(EventRecord.create(stream, "type", Map.of()));
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
            store.append(EventRecord.create(stream, "test", Map.of()));
        }

        store.truncate(stream, truncateFrom);

        List<EventRecord> events = store.fromStream(StreamName.parse(stream)).stream().collect(Collectors.toList());
        assertEquals(600, events.size());
        assertEquals(400, events.get(0).version);
    }

    @Test
    public void fromStreams_return_all_streams_based_on_the_position() {
        //given
        int numStreams = 1000;
        int numVersions = 50;
        String streamPrefix = "test-";
        for (int stream = 0; stream < numStreams; stream++) {
            for (int version = 1; version <= numVersions; version++) {
                store.append(EventRecord.create(streamPrefix + stream, "type", Map.of()));
            }
        }

        List<StreamName> streams = Stream.of("test-0", "test-1", "test-10", "test-100", "test-500").map(StreamName::parse).collect(Collectors.toList());

        Iterator<EventRecord> eventStream = store.fromStreams(new HashSet<>(streams), true);

        int foundEvents = 0;
        while (eventStream.hasNext()) {
            EventRecord event = eventStream.next();
            int streamIdx = foundEvents++ / numVersions;
            assertEquals(streams.get(streamIdx).name(), event.stream);
        }

        assertEquals(streams.size() * numVersions, foundEvents);
    }

    @Test(expected = NullPointerException.class)
    public void null_event_cannot_be_append() {
        store.append(null);
    }

    @Test
    public void appending_with_expected_version_appends_event() {
        EventRecord created = store.append(EventRecord.create("test-stream", "type", Map.of()), NO_EXPECTED_VERSION);
        assertEquals(0, created.version);
        created = store.append(EventRecord.create("test-stream", "type", Map.of()), NO_EXPECTED_VERSION);
        assertEquals(1, created.version);
    }

    @Test
    public void expected_no_event_version_in_the_newly_created_stream() {
        EventRecord created = store.append(EventRecord.create("test-stream", "type", Map.of()), NO_VERSION);
        assertEquals(0, created.version);
    }

    @Test
    public void get_returns_null_if_event_is_not_found() {
        EventRecord record = store.get(StreamName.of("some-stream", 0));
        assertNull(record);
    }

    @Test
    public void event_is_not_appended_if_version_mismatches() {
        String stream = "test-stream";
        try {
            store.append(EventRecord.create(stream, "type", Map.of()), 9999);
        } catch (Exception e) {
            assertTrue(e instanceof StreamException);
            assertEquals(NO_VERSION, store.version(stream));
            EventRecord record = store.get(StreamName.of(stream, 0));
            assertNull(record);
        }
    }

    @Test
    public void createStream_with_defaults() {
        var stream = "stream-123";
        store.createStream(stream);
        Optional<StreamInfo> streamInfo = store.streamMetadata(stream);
        assertTrue(streamInfo.isPresent());

        StreamInfo info = streamInfo.get();
        assertEquals(stream, info.name);
        assertEquals(NO_VERSION, info.version);
        assertEquals(NO_MAX_AGE, info.maxAge);
        assertEquals(StreamMetadata.NO_MAX_COUNT, info.maxCount);
    }

    @Test
    public void createStream_with_provided_values() {
        var stream = "stream-123";
        int maxCount = 2;
        int maxAge = 10;
        var metadata = Map.of("key1", "value1");
        var acl = Map.of("key1", 1);
        store.createStream(stream, maxCount, maxAge, acl, metadata);
        Optional<StreamInfo> streamInfo = store.streamMetadata(stream);
        assertTrue(streamInfo.isPresent());

        StreamInfo info = streamInfo.get();
        assertEquals(stream, info.name);
        assertEquals(NO_VERSION, info.version);
        assertEquals(maxAge, info.maxAge);
        assertEquals(maxCount, info.maxCount);
    }

    @Test
    public void fromStream_returns_all_entries_ordered_by_version() {
        String stream = "abc";
        int numStreams = 1000;
        for (int i = 0; i < numStreams; i++) {
            store.append(EventRecord.create(stream, "type-abc", Map.of()));
        }

        List<EventRecord> found = store.fromStream(StreamName.of(stream)).stream().collect(Collectors.toList());
        assertEquals(numStreams, found.size());
        int lastVersion = NO_VERSION;
        for (EventRecord record : found) {
            assertEquals(lastVersion + 1, record.version);
            lastVersion++;
        }
    }

    @Test
    public void fromStreams_returns_all_entries_ordered_by_version() {
        int numStreams = 1000;
        Set<StreamName> streams = new HashSet<>();
        Map<String, Integer> expectedVersions = new HashMap<>();
        for (int i = 0; i < numStreams; i++) {
            String sName = String.valueOf(i);
            streams.add(StreamName.of(sName));
            expectedVersions.put(sName, NO_VERSION);
            store.append(EventRecord.create(sName, "type-abc", Map.of()));
        }

        List<EventRecord> found = store.fromStreams(streams, true).stream().collect(Collectors.toList());
        assertEquals(numStreams, found.size());
        for (EventRecord record : found) {
            Integer version = expectedVersions.get(record.stream);
            assertEquals(version + 1, record.version);
            expectedVersions.put(record.stream, version + 1);
        }
    }

    @Test
    public void fromStream_returns_data_starting_from_given_version() {
        String stream = "abc";
        int numStreams = 1000;
        for (int i = 0; i < numStreams; i++) {
            store.append(EventRecord.create(stream, "type-abc", Map.of()));
        }

        int startVersion = 50;
        List<EventRecord> found = store.fromStream(StreamName.of(stream, startVersion)).stream().collect(Collectors.toList());
        assertEquals(numStreams - startVersion - 1, found.size());
        int lastVersion = startVersion;
        for (EventRecord record : found) {
            assertEquals(lastVersion + 1, record.version);
            lastVersion++;
        }
    }

    @Test
    public void stream_version_is_NO_VERSION_if_event_is_in_the_stream() {
        assertEquals(NO_VERSION, store.version("some-stream"));
    }

    @Test
    public void stream_version() {
        String stream = "stream-123";
        store.append(EventRecord.create(stream, "type", Map.of()));
        assertEquals(0, store.version(stream));

        store.append(EventRecord.create(stream, "type", Map.of()));
        assertEquals(1, store.version(stream));
    }
}