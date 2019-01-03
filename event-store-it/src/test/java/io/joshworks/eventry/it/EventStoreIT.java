package io.joshworks.eventry.it;

import io.joshworks.eventry.EventLogIterator;
import io.joshworks.eventry.EventStore;
import io.joshworks.eventry.IEventStore;
import io.joshworks.eventry.LinkToPolicy;
import io.joshworks.eventry.StreamName;
import io.joshworks.eventry.SystemEventPolicy;
import io.joshworks.eventry.data.IndexFlushed;
import io.joshworks.eventry.data.StreamCreated;
import io.joshworks.eventry.data.SystemStreams;
import io.joshworks.eventry.index.Range;
import io.joshworks.eventry.index.StreamHasher;
import io.joshworks.eventry.index.TableIndex;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.projections.State;
import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.fstore.core.hash.Murmur3Hash;
import io.joshworks.fstore.core.hash.XXHash;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.testutils.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class EventStoreIT {

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
    public void write_read() {

        final int size = 1000000;
        long start = System.currentTimeMillis();
        String stream = "test-stream";
        for (int i = 0; i < size; i++) {
            store.append(EventRecord.create(stream, "" + i, "body-" + i));
        }
        System.out.println("WRITE: " + (System.currentTimeMillis() - start));


        Iterator<EventRecord> events = store.fromStream(StreamName.of(stream));
        int found = 0;

        start = System.currentTimeMillis();
        while (events.hasNext()) {
            EventRecord next = events.next();
            found++;
        }
        System.out.println("READ: " + (System.currentTimeMillis() - start));

        if (found != size) {
            throw new RuntimeException("Expected " + size + " Got " + found);
        }
    }

    @Test
    public void query() {
        final int size = 100;
        String stream1 = "test-stream";
        String stream2 = "test-stream2";
        for (int i = 0; i < size; i++) {
            store.append(EventRecord.create(stream1, "" + i, "{\"value\": " + i + "}"));
        }
        for (int i = 0; i < size; i++) {
            store.append(EventRecord.create(stream2, "" + i, "{\"value\": " + i + "}"));
        }

        State state = new State();
        state.put("stream1", 0L);
        state.put("stream2", 0L);
        State result = store.query(Set.of(stream1, stream2), state, "if (stream === 'test-stream') state.stream1 += timestamp; else state.stream2 += version;");
        System.out.println(result);
    }

    @Test
    public void insert_1M_same_stream() {


        long start = System.currentTimeMillis();
        int size = 1000000;

        String stream = "test-stream";
        for (int i = 0; i < size; i++) {
            store.append(EventRecord.create(stream, "" + i, "body-" + i));
        }

        System.out.println("WRITE: " + (System.currentTimeMillis() - start));

        start = System.currentTimeMillis();

        Stream<EventRecord> events = store.fromStream(StreamName.of(stream)).stream();

        System.out.println("SIZE: " + events.count());
        System.out.println("READ: " + (System.currentTimeMillis() - start));

        store.close();
    }

    @Test
    public void insert_1M_unique_streams() {

        long start = System.currentTimeMillis();
        int size = 1000000;

        String streamPrefix = "test-stream-";
        for (int i = 0; i < size; i++) {
            store.append(EventRecord.create(streamPrefix + i, "" + i, "body-" + i));
        }
        System.out.println("WRITE: " + (System.currentTimeMillis() - start));

        start = System.currentTimeMillis();

        for (int i = 0; i < size; i++) {
            Stream<EventRecord> events = store.fromStream(StreamName.of(streamPrefix + i)).stream();
            assertEquals("Failed on iteration " + i, 1, events.count());
            if(i % 10000 == 0) {
                System.out.println("Processed " + i);
            }
        }

        System.out.println("READ: " + (System.currentTimeMillis() - start));
    }

    @Test
    public void insert_100000_streams_with_1_version_each() {
        testWith(100000, 1);
    }

    @Test
    public void insert_500000_streams_with_1_version_each() {
        testWith(500000, 1);
    }

    @Test
    public void insert_2000000_streams_with_1_version_each() {
        testWith(2000000, 1);
    }

    @Test
    public void insert_100000_events_of_same_stream() {
        testWith(1, 100000);
    }

    @Test
    public void insert_500000_events_of_same_stream() {
        testWith(1, 500000);
    }

    @Test
    public void insert_2000000_events_of_same_stream() {
        testWith(1, 2000000);
    }

    @Test
    public void insert_1000_streams_with_1000_version_each() {
        testWith(1000, 1000);
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
                Stream<EventRecord> events = store.fromStream(StreamName.of(streamPrefix + i)).stream();
                assertEquals("Failed on iteration " + i, 1, events.count());
            }
        }
    }

    @Test
    public void fromAll_returns_all_items_when_store_is_reopened() {

        //given
        int size = 10000;
        String streamPrefix = "test-stream-";
        for (int i = 0; i < size; i++) {
            store.append(EventRecord.create(streamPrefix + i, "" + i, "body-" + i));
        }

        store.close();

        try (IEventStore store = EventStore.open(directory)) {
            Stream<EventRecord> events = store.fromAll(LinkToPolicy.RESOLVE, SystemEventPolicy.INCLUDE).stream();
            assertTrue(events.count() >= size);
        }
    }

    @Test
    public void get_returns_all_items_when_store_is_reopened() {

        //given
        int size = 10000;
        String streamPrefix = "test-stream-";
        for (int i = 0; i < size; i++) {
            store.append(EventRecord.create(streamPrefix + i, "" + i, "body-" + i));
        }

        store.close();

        try (IEventStore store = EventStore.open(directory)) {
            for (int i = 0; i < size; i++) {
                EventRecord event = store.get(StreamName.of(streamPrefix + i, Range.START_VERSION));
                assertNotNull(event);
                assertEquals(Range.START_VERSION, event.version);
                assertEquals(streamPrefix + i, event.stream);
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

        LogIterator<EventRecord> iter = store.fromStream(StreamName.of(stream));

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
    public void events_have_stream_and_version() {
        int numStreams = 1000;
        String streamPrefix = "stream-";
        for (int i = 0; i < numStreams; i++) {
            store.append(EventRecord.create(streamPrefix + i, String.valueOf(i), "body-" + i));
        }

        int version = 0;
        for (int i = 0; i < numStreams; i++) {
            String stream = streamPrefix + i;
            EventRecord event = store.get(StreamName.of(streamPrefix + i, version));

            assertNotNull(event);
            assertEquals(stream, event.stream);
            assertEquals(version, event.version);
        }
    }

    @Test
    public void iterator() {
        //given
        int numEvents = 10000;
        String streamPrefix = "test-";
        for (int i = 0; i < numEvents; i++) {
            store.append(EventRecord.create(streamPrefix + i, String.valueOf(i), "body-" + i));
        }

        //when
        for (int i = 0; i < numEvents; i++) {
            Iterator<EventRecord> events = store.fromStream(StreamName.of(streamPrefix + i));

            assertTrue(events.hasNext());

            int size = 0;
            while (events.hasNext()) {
                EventRecord event = events.next();
                assertEquals("Wrong event body, iteration: " + i, "body-" + i, new String(event.body, StandardCharsets.UTF_8));
                assertEquals(String.valueOf(i), event.type);
                size++;
            }

            //then
            assertEquals(1, size);
        }
    }

    @Test
    public void fromStreams_return_all_streams_based_on_the_position() {
        //given
        int numStreams = 10000;
        int numVersions = 50;
        String streamPrefix = "test-";
        for (int stream = 0; stream < numStreams; stream++) {
            for (int version = 1; version <= numVersions; version++) {
                store.append(EventRecord.create(streamPrefix + stream, String.valueOf("type"), "body-" + stream));
            }
        }

        List<StreamName> streams = Stream.of("test-0", "test-1", "test-10", "test-100", "test-1000").map(StreamName::of).collect(Collectors.toList());

        Iterator<EventRecord> eventStream = store.fromStreams(new HashSet<>(streams));

        int eventCounter = 0;
        while (eventStream.hasNext()) {
            EventRecord event = eventStream.next();
            int streamIdx = eventCounter++ / numVersions;
            assertEquals(streams.get(streamIdx), event.stream);
        }

        assertEquals(streams.size() * numVersions, eventCounter);
    }

    @Test
    public void fromStream_without_version_starts_from_zero() throws IOException {
        String stream = "stream-1";
        store.append(EventRecord.create(stream, "test", "body1"));
        store.append(EventRecord.create(stream, "test", "body2"));

        try(var it = store.fromStream(StreamName.of(stream))) {
            assertTrue(it.hasNext());
            assertEquals(0, it.next().version);

            assertTrue(it.hasNext());
            assertEquals(1, it.next().version);
        }
    }

    @Test
    public void fromStreams_without_version_starts_from_zero() throws IOException {
        String stream1 = "stream-1";
        String stream2 = "stream-2";
        store.append(EventRecord.create(stream1, "test", "body1"));
        store.append(EventRecord.create(stream2, "test", "body2"));

        try(var it = store.fromStreams(Set.of(StreamName.of(stream1), StreamName.of(stream2)))) {
            assertTrue(it.hasNext());
            assertEquals(0, it.next().version);

            assertTrue(it.hasNext());
            assertEquals(0, it.next().version);
        }
    }

    @Test
    public void fromAll_return_all_elements_in_insertion_order() {

        int size = 1000;
        for (int i = 0; i < size; i++) {
            store.append(EventRecord.create("stream-" + i, "test", "body"));
        }

        Iterator<EventRecord> it = store.fromAll(LinkToPolicy.RESOLVE, SystemEventPolicy.INCLUDE);

        EventRecord last = null;
        while (it.hasNext()) {
            EventRecord next = it.next();
            if (last == null)
                last = next;
            assertTrue(next.timestamp >= last.timestamp); //no order is guaranteed for same timestamps
        }
    }

    @Test
    public void fromStreams_single_stream() {
        //given
        String streamPrefix = "test-";
        int numStreams = 10000;
        int numVersions = 50;
        for (int stream = 0; stream < numStreams; stream++) {
            for (int version = 0; version < numVersions; version++) {
                store.append(EventRecord.create(streamPrefix + stream, "type", "body-" + stream));
            }
        }

        StreamName eventToQuery = StreamName.of("test-1");
        Iterator<EventRecord> eventStream = store.fromStreams(Set.of(eventToQuery));

        int eventCounter = 0;
        while (eventStream.hasNext()) {
            EventRecord event = eventStream.next();
            assertEquals(eventToQuery.name(), event.stream);
            eventCounter++;
        }

        assertEquals(numVersions, eventCounter);
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

        Iterator<EventRecord> eventStream = store.fromStream(StreamName.of(stream));

        int eventCounter = 0;
        while (eventStream.hasNext()) {
            EventRecord event = eventStream.next();
            assertTrue(event.version >= numVersions - maxCount);
            eventCounter++;
        }

        assertEquals(maxCount, eventCounter);
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

        long count = store.fromStream(StreamName.of(stream)).stream().count();
        assertEquals(numVersions, count);

        Thread.sleep(maxAgeSeconds * 1000);

        count = store.fromStream(StreamName.of(stream)).stream().count();
        assertEquals(numVersions, count);
    }

    @Test
    public void linkTo() {
        int size = 1000000;

        System.out.println("Creating entries");
        String streamName = "test-stream";
        for (int i = 0; i < size; i++) {
            store.append(EventRecord.create(streamName, "test", UUID.randomUUID().toString().substring(0, 8)));
        }

//        LogDump.dumpIndex(new File("index1.txt"), store);

        System.out.println("LinkTo 1");
        store.fromStream(StreamName.of(streamName)).stream().forEach(e -> {
            String firstLetter = Arrays.toString(e.body).substring(0, 1);
            store.linkTo("letter-" + firstLetter, e);
        });

//        LogDump.dumpIndex(new File("index2.txt"), store);
        System.out.println("LinkTo 2");
        store.fromStream(StreamName.of(streamName)).stream().forEach(e -> {
            String firstLetter = Arrays.toString(e.body).substring(0, 2);
            store.linkTo("letter-" + firstLetter, e);
        });

        System.out.println("LinkTo 3");
        store.fromStream(StreamName.of(streamName)).stream().forEach(e -> {
            String firstLetter = Arrays.toString(e.body).substring(0, 3);
            store.linkTo("letter-" + firstLetter, e);
        });


    }

    //This isn't an actual test, it's more a debugging tool
    @Test
    public void hash_collision() {
        Map<Long, String> hashes = new HashMap<>();
        StreamHasher hasher = new StreamHasher(new XXHash(), new Murmur3Hash());

        for (int i = 0; i < 10000000; i++) {
            String value = "test-stream-" + i;
            long hash = hasher.hash(value);
            if (hashes.containsKey(hash)) {
                fail("Hash collision: " + hashes.get(hash) + " -> " + value);
            }
            hashes.put(hash, value);
        }
    }

    @Test
    public void get() {
        int size = 1000;
        String streamPrefix = "stream-";
        for (int i = 0; i < size; i++) {
            store.append(EventRecord.create(streamPrefix + i, "test", "body"));
        }

        for (int i = 0; i < size; i++) {
            String stream = streamPrefix + i;
            EventRecord event = store.get(StreamName.of(stream, 0));
            assertNotNull(event);
            assertEquals(stream, event.stream);
            assertEquals(0, event.version);
        }
    }

    @Test
    public void many_streams_linkTo() {
        int size = 1200000;
        String allStream = "all";
        for (int i = 0; i < size; i++) {
            store.append(EventRecord.create(allStream, "test", UUID.randomUUID().toString()));
        }

        int numOtherIndexes = 5;

        assertEquals(size, store.fromStream(StreamName.of(allStream)).stream().count());

        IntStream.range(0, numOtherIndexes).forEach(i -> {
            long start = System.currentTimeMillis();
            store.fromStream(StreamName.of(allStream)).stream().forEach(e -> store.linkTo(String.valueOf(i), e));
            System.out.println("LinkTo " + size + " events to stream " + i + " in " + (System.currentTimeMillis() - start));
        });


        assertEquals(size, store.fromStream(StreamName.of(allStream)).stream().count());

        for (int i = 0; i < numOtherIndexes; i++) {
            String streamName = String.valueOf(i);
            long foundSize = store.fromStream(StreamName.of(streamName)).stream().count();
            assertEquals("Failed on iteration: " + i, size, foundSize);
        }
    }

    @Test
    public void fromStreamsStartingWith_returns_orderedEvents() {
        //given
        int numStreams = 1000;
        int numVersions = 50;
        String streamPrefix = "test-";

        for (int stream = 0; stream < numStreams; stream++) {
            for (int version = 1; version <= numVersions; version++) {
                store.append(EventRecord.create(streamPrefix + stream, String.valueOf("type"), "body-" + stream));
            }
        }

        //some other stream we don't care about
        for (int version = 1; version <= numVersions; version++) {
            store.append(EventRecord.create("someOtherStream", String.valueOf("type"), "body-" + version));
        }

        Iterator<EventRecord> eventStream = store.fromStreams(streamPrefix + "*");

        assertTrue(eventStream.hasNext());

        int eventCounter = 0;
        while (eventStream.hasNext()) {
            EventRecord event = eventStream.next();
            eventCounter++;
            assertTrue(event.stream.startsWith(streamPrefix));
        }

        assertEquals(numStreams * numVersions, eventCounter);
    }

//    @Test
//    public void poller_returns_all_items() throws IOException, InterruptedException {
//
//        int items = 1000000;
//        String stream = "stream-123";
//        for (int i = 0; i < items; i++) {
//            store.append(EventRecord.create(stream, "type", "body"));
//            if (i % 10000 == 0) {
//                System.out.println("WRITE: " + i);
//            }
//        }
//
//        System.out.println("Write completed");
//        try (PollingSubscriber<EventRecord> streamPoller = store.streamPoller(stream)) {
//            for (int i = 0; i < items; i++) {
//                EventRecord event = streamPoller.take();
//                assertNotNull(event);
//                assertEquals(i, event.version);
//                if (i % 10000 == 0) {
//                    System.out.println("READ: " + i);
//                }
//            }
//        }
//    }
//
//    @Test
//    public void poller_with_multiple_streams_returns_all_items() throws IOException, InterruptedException {
//
//        int items = 1000000;
//        Set<String> allStreams = new HashSet<>();
//        for (int i = 0; i < items; i++) {
//            String stream = "stream-" + i;
//            allStreams.add(stream);
//            store.append(EventRecord.create(stream, "type", "body"));
//            if (i % 10000 == 0) {
//                System.out.println("WRITE: " + i);
//            }
//        }
//
//        System.out.println("Write completed");
//        //Orders of events is not guaranteed across streams
//        try (PollingSubscriber<EventRecord> streamPoller = store.streamPoller(allStreams)) {
//            for (int i = 0; i < items; i++) {
//                EventRecord event = streamPoller.take();
//                assertNotNull(event);
//                assertEquals("Failed on iteration:" + i + " event:" + event, 0, event.version);
//
//
//                if (i % 10000 == 0) {
//                    System.out.println("READ: " + i);
//                }
//            }
//        }
//    }

    @Test
    public void linkTo_events_are_resolved_on_get() {

        var original = "stream-1";
        var linkToStream = "stream-2";
        var created = store.append(EventRecord.create(original, "type", "body"));

        store.linkTo(linkToStream, created);

        EventRecord eventRecord = store.get(StreamName.of(linkToStream, 0));
        assertNotNull(eventRecord);
        assertEquals(original, eventRecord.stream);
        assertEquals(0, eventRecord.version);

    }

    @Test
    public void index_is_loaded_with_non_persisted_entries() {

        //given
        String stream = "stream-1";
        //1 segment + in memory
        int size = TableIndex.DEFAULT_FLUSH_THRESHOLD + (TableIndex.DEFAULT_FLUSH_THRESHOLD / 2);
        for (int i = 0; i < size; i++) {
            store.append(EventRecord.create(stream, "type-1", "body-" + 1));
        }

        store.close();
        store = EventStore.open(directory);

        EventLogIterator iterator = store.fromStream(StreamName.of(stream));

        int total = 0;
        for (int i = 0; i < size; i++) {
            if(i == 999997) {
                System.out.println("");
            }
            assertTrue(iterator.hasNext());
            EventRecord next = iterator.next();
            assertEquals(i, next.version);
            total++;
        }
        assertEquals(size, total);
    }


    @Test
    public void index_flush_generates_INDEX_FLUSHED_event() {

        //given
        String stream = "stream-1";
        int size = TableIndex.DEFAULT_FLUSH_THRESHOLD + (TableIndex.DEFAULT_FLUSH_THRESHOLD / 2);
        for (int i = 0; i < size; i++) {
            store.append(EventRecord.create(stream, "type-1", "body-" + 1));
        }

        List<EventRecord> indexEvents = store.fromStream(StreamName.of(SystemStreams.INDEX)).stream().collect(Collectors.toList());
        assertEquals(1, indexEvents.size());
        var record = indexEvents.get(0);
        var indexFlushed = IndexFlushed.from(record);
        assertEquals(TableIndex.DEFAULT_FLUSH_THRESHOLD, indexFlushed.entries);
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
    public void all_streams_are_loaded_on_reopen() {

        int size = 3000000;
        var createdStreams = new HashSet<String>();
        for (int i = 0; i < size; i++) {
            var stream = "stream-" + i;
            createdStreams.add(stream);
            store.append(EventRecord.create(stream, "test", "body"));
        }

        store.close();

        store = EventStore.open(directory);

        List<EventRecord> foundStreams = store.fromStream(StreamName.of(SystemStreams.STREAMS)).stream().collect(Collectors.toList());

        //stream also contains the stream definition itself
        assertTrue(foundStreams.size() >= createdStreams.size());

        int hits = 0;
        for (EventRecord streamRecord : foundStreams) {
            StreamMetadata streamMetadata = StreamCreated.from(streamRecord);
            if(createdStreams.contains(streamMetadata.name)) {
                hits++;
            }
        }
        assertEquals(createdStreams.size(), hits);
    }

    @Test
    public void fromStream_returns_items_after_truncated_version() {

        int size = 1000;
        int truncateBeforeVersion = 400;
        String stream ="stream-123";
        for (int i = 0; i < size; i++) {
            store.append(EventRecord.create(stream, "test", "body"));
        }

        store.truncate(stream, truncateBeforeVersion);

        List<EventRecord> events = store.fromStream(StreamName.of(stream)).stream().collect(Collectors.toList());
        assertEquals(600, events.size());
        assertEquals(400, events.get(0).version);
    }

    @Test
    public void compaction_removes_truncated_entries() {

        int size = 3000000;
        int truncateBeforeVersion = 400;
        String stream ="stream-123";
        for (int i = 0; i < size; i++) {
            store.append(EventRecord.create(stream, "test", "body"));
        }

        store.truncate(stream, truncateBeforeVersion);


        long count = store.fromStream(StreamName.of(stream)).stream().count();
        assertEquals(size - truncateBeforeVersion , count);
    }

    private void testWith(int streams, int numVersionPerStream) {
        long start = System.currentTimeMillis();

        String streamPrefix = "test-stream-";

        for (int stream = 0; stream < streams; stream++) {
            for (int version = 1; version <= numVersionPerStream; version++) {
                String streamName = streamPrefix + stream;
                try {
                    store.append(EventRecord.create(streamName, "" + stream, "body-" + stream));
                } catch (Exception e) {
                    throw new RuntimeException("Failed on stream " + streamName, e);
                }
            }
        }
        System.out.println("WRITE: " + (System.currentTimeMillis() - start));


        start = System.currentTimeMillis();
        for (int stream = 0; stream < streams; stream++) {
            String streamName = streamPrefix + stream;
            try {
                //VERSION
                int foundVersion = store.version(streamName);
                assertEquals(numVersionPerStream - 1, foundVersion);

                //FROM STREAM
                try (Stream<EventRecord> events = store.fromStream(StreamName.of(streamName)).stream()) {
                    assertEquals(numVersionPerStream, events.collect(Collectors.toList()).size());

                    for (int version = 0; version < numVersionPerStream; version++) {
                        //GET
                        EventRecord event = store.get(StreamName.of(streamName, version));
                        assertNotNull(event);
                        assertEquals(streamName, event.stream);
                        assertEquals(version, event.version);
                    }
                }


            } catch (Exception e) {
                throw new RuntimeException("Failed on stream " + streamName, e);
            }
        }
        System.out.println("READ: " + (System.currentTimeMillis() - start));

    }

}
