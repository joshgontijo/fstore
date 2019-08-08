package io.joshworks.eventry;

import io.joshworks.eventry.api.EventStoreIterator;
import io.joshworks.eventry.api.IEventStore;
import io.joshworks.fstore.es.shared.streams.SystemStreams;
import io.joshworks.fstore.es.shared.EventRecord;
import io.joshworks.eventry.stream.StreamException;
import io.joshworks.eventry.stream.StreamInfo;
import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.fstore.core.util.FileUtils;
import io.joshworks.fstore.es.shared.EventId;
import io.joshworks.fstore.es.shared.EventMap;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static io.joshworks.fstore.es.shared.EventId.NO_EXPECTED_VERSION;
import static io.joshworks.fstore.es.shared.EventId.NO_VERSION;
import static io.joshworks.eventry.stream.StreamMetadata.NO_MAX_AGE;
import static io.joshworks.eventry.stream.StreamMetadata.NO_MAX_COUNT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

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

        EventRecord record = store.get(EventId.of(SystemStreams.STREAMS, 0));
        assertNotNull(record);
        assertEquals(0, record.version);
        assertEquals(SystemStreams.STREAMS, record.stream);

        List<EventRecord> stream = store.fromStream(EventId.of(SystemStreams.STREAMS)).toList();
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
                Stream<EventRecord> events = store.fromStream(EventId.parse(streamPrefix + i)).stream();
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

        EventStoreIterator iter = store.fromStream(EventId.parse(stream));

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

        Iterator<EventRecord> eventStream = store.fromStream(EventId.parse(stream));

        int eventCounter = 0;
        while (eventStream.hasNext()) {
            EventRecord event = eventStream.next();
            assertTrue("Failed on " + event, event.version >= numVersions - maxCount);
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

        long count = store.fromStream(EventId.parse(stream)).stream().count();
        assertEquals(40, count);

        Iterator<EventRecord> eventStream = store.fromStream(EventId.parse(stream));

        int expectedVersion = 60;
        while (eventStream.hasNext()) {
            EventRecord event = eventStream.next();
            assertEquals(expectedVersion, event.version);
            expectedVersion++;
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void stream_name_cannot_start_with_system_reserved_prefix() {
        store.append(EventRecord.create(EventId.SYSTEM_PREFIX + "stream", "a", Map.of()));
    }

    @Test(expected = IllegalArgumentException.class)
    public void event_type_is_mandatory() {
        store.append(EventRecord.create("stream", null, Map.of()));
    }

    @Test
    public void fromStream_returns_data_only_when_within_maxAge() throws InterruptedException {
        String stream = "test-stream";
        int maxAgeSeconds = 2;
        int numVersions = 50;
        store.createStream(stream, NO_MAX_COUNT, maxAgeSeconds);

        for (int version = 0; version < numVersions; version++) {
            store.append(EventRecord.create(stream, "type", Map.of()));
        }

        Thread.sleep(TimeUnit.SECONDS.toMillis(maxAgeSeconds + 1));

        long count = store.fromStream(EventId.of(stream)).stream().count();
        assertEquals(0, count);
    }

    @Test
    public void fromStream_does_not_return_expired_linkTo() throws InterruptedException {
        String stream = "test-stream";
        String linkToStream = "linkTo-stream";
        int maxAgeSeconds = 2;
        int numVersions = 50;
        store.createStream(stream);
        store.createStream(linkToStream, NO_MAX_COUNT, maxAgeSeconds);

        for (int version = 0; version < numVersions; version++) {
            EventRecord event = store.append(EventRecord.create(stream, "type", Map.of()));
            store.linkTo(linkToStream, event);
        }

        Thread.sleep(TimeUnit.SECONDS.toMillis(maxAgeSeconds + 1));

        long count = store.fromStream(EventId.of(linkToStream)).stream().count();
        assertEquals(0, count);
    }

    @Test
    public void fromStream_does_not_return_expired_data_from_iterator_acquire_before_entry_insertion() throws InterruptedException {
        String stream = "test-stream";
        int maxAgeSeconds = 2;
        int numVersions = 50;
        store.createStream(stream, NO_MAX_COUNT, maxAgeSeconds);

        for (int version = 0; version < numVersions; version++) {
            store.append(EventRecord.create(stream, "type", Map.of()));
        }

        EventStoreIterator iterator = store.fromStream(EventId.parse(stream));
        Thread.sleep(TimeUnit.SECONDS.toMillis(maxAgeSeconds + 1));

        long count = iterator.stream().count();
        assertEquals(0, count);
    }

    @Test
    public void fromStream_acquired_before_truncation_does_not_return_data_after_truncating() {
        String stream = "test-stream";
        int numVersions = 50;
        int truncatedBefore = 30;

        for (int version = 0; version < numVersions; version++) {
            store.append(EventRecord.create(stream, "type", Map.of()));
        }

        EventStoreIterator iterator = store.fromStream(EventId.parse(stream));

        store.truncate(stream, truncatedBefore);

        long count = iterator.stream().count();
        assertEquals(numVersions - truncatedBefore - 1, count);
    }

    @Test
    public void fromStreams_returns_data_within_maxAge() throws InterruptedException {
        String stream = "stream-1";
        int maxAgeSeconds = 2;
        int numVersions = 50;
        store.createStream(stream, NO_MAX_COUNT, maxAgeSeconds);

        for (int version = 0; version < numVersions; version++) {
            store.append(EventRecord.create(stream, "type", Map.of()));
        }

        Thread.sleep(TimeUnit.SECONDS.toMillis(maxAgeSeconds + 1));

        long count = store.fromStreams(EventMap.from(EventId.of(stream))).stream().count();
        assertEquals(0, count);
    }

    @Test
    public void fromStreams_returns_data_within_maxAge_with_iterator_created_before_the_expire_time() throws InterruptedException {
        String stream = "test-stream";
        int maxAgeSeconds = 2;
        int numVersions = 50;
        store.createStream(stream, NO_MAX_COUNT, maxAgeSeconds);
        EventStoreIterator iterator = store.fromStreams(EventMap.from(EventId.of(stream)));

        for (int version = 0; version < numVersions; version++) {
            store.append(EventRecord.create(stream, "type", Map.of()));
        }

        Thread.sleep(TimeUnit.SECONDS.toMillis(maxAgeSeconds + 1));

        long count = iterator.stream().count();
        assertEquals(0, count);
    }

    @Test
    public void fromStreams_PATTERN_does_not_return_expired_data() throws InterruptedException {
        String stream1 = "stream-1";
        String stream2 = "stream-2";
        int maxAgeSeconds = 2;
        int numVersions = 50;
        store.createStream(stream1, NO_MAX_COUNT, maxAgeSeconds);
        store.createStream(stream2, NO_MAX_COUNT, maxAgeSeconds);

        for (int version = 0; version < numVersions; version++) {
            store.append(EventRecord.create(stream1, "type", Map.of()));
        }

        for (int version = 0; version < numVersions; version++) {
            store.append(EventRecord.create(stream2, "type", Map.of()));
        }

        Thread.sleep(TimeUnit.SECONDS.toMillis(maxAgeSeconds + 1));

        long count = store.fromStreams(EventMap.empty(), Set.of("stream-")).stream().count();
        assertEquals(0, count);
    }

    @Test
    public void fromStreamsPATTERN_returns_data_within_maxAge_with_iterator_created_before_the_expire_time() throws InterruptedException {
        String stream = "test-stream";
        int maxAgeSeconds = 2;
        int numVersions = 50;
        store.createStream(stream, NO_MAX_COUNT, maxAgeSeconds);

        for (int version = 0; version < numVersions; version++) {
            store.append(EventRecord.create(stream, "type", Map.of()));
        }

        EventStoreIterator iterator = store.fromStreams(EventMap.empty(), Set.of("stream-"));
        Thread.sleep(TimeUnit.SECONDS.toMillis(maxAgeSeconds + 1));

        long count = iterator.stream().count();
        assertEquals(0, count);
    }

    @Test
    public void get_returns_resolved_event() {
        String linkToStream = "link-to-stream";
        EventRecord original = store.append(EventRecord.create("original", "a", Map.of()));
        EventRecord linkTo = store.linkTo(linkToStream, original);

        EventRecord found = store.get(EventId.of(linkTo.stream, linkTo.version));
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

        List<EventRecord> events = store.fromStream(EventId.parse(stream)).toList();
        assertEquals(600, events.size());
        assertEquals(400, events.get(0).version);
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
        EventRecord record = store.get(EventId.of("some-stream", 0));
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
            EventRecord record = store.get(EventId.of(stream, 0));
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

        List<EventRecord> found = store.fromStream(EventId.of(stream)).toList();
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
        Set<EventId> streams = new HashSet<>();
        Map<String, Integer> expectedVersions = new HashMap<>();
        for (int i = 0; i < numStreams; i++) {
            String sName = String.valueOf(i);
            streams.add(EventId.of(sName));
            expectedVersions.put(sName, NO_VERSION);
            store.append(EventRecord.create(sName, "type-abc", Map.of()));
        }

        List<EventRecord> found = store.fromStreams(EventMap.from(streams)).toList();
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
        List<EventRecord> found = store.fromStream(EventId.of(stream, startVersion)).toList();
        assertEquals(numStreams - startVersion - 1, found.size());
        int lastVersion = startVersion;
        for (EventRecord record : found) {
            assertEquals(lastVersion + 1, record.version);
            lastVersion++;
        }
    }

    @Test
    public void fromStream_returns_data_when_instantiated_after_creating_stream_and_before_appending_event() {
        String stream = "abc";

        store.createStream(stream);
        EventStoreIterator iterator = store.fromStream(EventId.of(stream));
        store.append(EventRecord.create(stream, "type-abc", Map.of()));

        assertTrue(iterator.hasNext());
    }

    @Test
    public void fromStream_returns_data_when_instantiated_before_creating_stream_and_before_appending_event() {
        String stream = "abc";

        EventStoreIterator iterator = store.fromStream(EventId.of(stream));
        store.append(EventRecord.create(stream, "type-abc", Map.of()));

        assertTrue(iterator.hasNext());
    }

    @Test
    public void fromStreams_returns_data_when_instantiated_after_creating_stream_and_before_appending_event() {
        String stream1 = "abc";
        String stream2 = "def";


        store.createStream(stream1);
        store.createStream(stream2);

        EventMap streams = EventMap.from(EventId.of(stream1)).add(EventId.of(stream2));
        EventStoreIterator iterator = store.fromStreams(streams);

        store.append(EventRecord.create(stream1, "type-abc", Map.of()));
        assertTrue(iterator.hasNext());
        assertEquals(stream1, iterator.next().stream);

        store.append(EventRecord.create(stream2, "type-abc", Map.of()));
        assertTrue(iterator.hasNext());
        assertEquals(stream2, iterator.next().stream);
    }

    @Test
    public void fromStreams_returns_data_when_instantiated_before_creating_stream_and_before_appending_event() {
        String stream1 = "abc";
        String stream2 = "def";

        EventMap streams = EventMap.from(EventId.of(stream1)).add(EventId.of(stream2));

        EventStoreIterator iterator = store.fromStreams(streams);

        store.append(EventRecord.create(stream1, "type-abc", Map.of()));
        assertTrue(iterator.hasNext());
        assertEquals(stream1, iterator.next().stream);

        store.append(EventRecord.create(stream2, "type-abc", Map.of()));
        assertTrue(iterator.hasNext());
        assertEquals(stream2, iterator.next().stream);

    }

    @Test
    public void fromStreamsPATTERN_returns_data_when_instantiated_after_creating_stream_and_before_appending_event() {
        String stream1 = "stream-abc";
        String stream2 = "stream-def";
        String pattern = "stream-*";

        store.createStream(stream1);
        store.createStream(stream2);

        EventStoreIterator iterator = store.fromStreams(EventMap.empty(), Set.of(pattern));

        store.append(EventRecord.create(stream1, "type-abc", Map.of()));
        assertTrue(iterator.hasNext());
        assertEquals(stream1, iterator.next().stream);

        store.append(EventRecord.create(stream2, "type-abc", Map.of()));
        assertTrue(iterator.hasNext());
        assertEquals(stream2, iterator.next().stream);
    }

    @Test
    public void fromStreamsPATTERN_returns_data_when_instantiated_before_creating_stream_and_before_appending_event() {
        String pattern = "stream-*";
        String stream1 = "stream-1";
        String stream2 = "stream-2";

        EventStoreIterator iterator = store.fromStreams(EventMap.empty(), Set.of(pattern));

        store.append(EventRecord.create(stream1, "type-abc", Map.of()));
        assertTrue(iterator.hasNext());
        assertEquals(stream1, iterator.next().stream);

        store.append(EventRecord.create(stream2, "type-abc", Map.of()));
        assertTrue(iterator.hasNext());
        assertEquals(stream2, iterator.next().stream);

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

    @Test
    public void count_returns_zero_when_no_stream_is_present() {
        assertEquals(0, store.count("stream-123"));
    }

    @Test
    public void count_returns_correct_count_for_a_stream() {
        String stream = "stream-123";
        store.append(EventRecord.create(stream, "type", Map.of()));
        store.append(EventRecord.create(stream, "type", Map.of()));
        store.append(EventRecord.create(stream, "type", Map.of()));
        store.append(EventRecord.create(stream, "type", Map.of()));
        store.append(EventRecord.create(stream, "type", Map.of()));

        assertEquals(5, store.count(stream));
    }

    @Test
    public void count_returns_correct_count_for_truncated_stream() {
        String stream = "stream-123";
        store.append(EventRecord.create(stream, "type", Map.of()));
        store.append(EventRecord.create(stream, "type", Map.of()));
        store.append(EventRecord.create(stream, "type", Map.of()));
        store.append(EventRecord.create(stream, "type", Map.of()));
        store.append(EventRecord.create(stream, "type", Map.of()));

        store.truncate(stream, 1);
        store.fromStream(EventId.parse(stream)).forEachRemaining(System.out::println);
        assertEquals(3, store.count(stream));
    }
}