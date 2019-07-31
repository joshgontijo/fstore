package io.joshworks.eventry.it;

import io.joshworks.eventry.EventStore;
import io.joshworks.eventry.LinkToPolicy;
import io.joshworks.eventry.StreamName;
import io.joshworks.eventry.SystemEventPolicy;
import io.joshworks.eventry.api.EventStoreIterator;
import io.joshworks.eventry.api.IEventStore;
import io.joshworks.eventry.data.StreamCreated;
import io.joshworks.eventry.data.SystemStreams;
import io.joshworks.eventry.index.StreamHasher;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.fstore.core.hash.Murmur3Hash;
import io.joshworks.fstore.core.hash.XXHash;
import io.joshworks.fstore.core.util.FileUtils;
import io.joshworks.fstore.core.util.Threads;
import io.joshworks.fstore.log.iterators.Iterators;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.joshworks.eventry.StreamName.START_VERSION;
import static io.joshworks.eventry.stream.StreamMetadata.NO_MAX_AGE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class EventStoreIT {

    private File directory;
    private EventStore store;

    @Before
    public void setUp() {
        directory = FileUtils.testFolder();
        store = EventStore.open(directory);
    }

    @After
    public void tearDown() {
//        store.close();
//        FileUtils.tryDelete(directory);
    }

    @Test
    public void write_read() {

        final int size = 1000000;
        long start = System.currentTimeMillis();
        String stream = "test-stream";
        for (int i = 0; i < size; i++) {
            store.append(EventRecord.create(stream, "" + i, Map.of()));
        }
        System.out.println("WRITE: " + (System.currentTimeMillis() - start));


        Iterator<EventRecord> events = store.fromStream(StreamName.parse(stream));
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
    public void insert_1M_same_stream() {
        long start = System.currentTimeMillis();
        int size = 1000000;

        String stream = "test-stream";
        for (int i = 0; i < size; i++) {
            store.append(EventRecord.create(stream, "" + i, Map.of()));
        }

        System.out.println("WRITE: " + (System.currentTimeMillis() - start));

        start = System.currentTimeMillis();

        Stream<EventRecord> events = store.fromStream(StreamName.parse(stream)).stream();

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
            store.append(EventRecord.create(streamPrefix + i, "" + i, Map.of()));
        }
        System.out.println("WRITE: " + (System.currentTimeMillis() - start));

        start = System.currentTimeMillis();

        for (int i = 0; i < size; i++) {
            Stream<EventRecord> events = store.fromStream(StreamName.parse(streamPrefix + i)).stream();
            assertEquals("Failed on iteration " + i, 1, events.count());
            if (i % 10000 == 0) {
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
        testWith(10000, 1000);
    }

    @Test
    public void insert_1_streams_with_1000_version_each_maxCount() {

        String streamName = "stream-123";
        store.createStream(streamName, 10000, NO_MAX_AGE);
        for (int version = 1; version <= 500000; version++) {
            try {
                store.append(EventRecord.create(streamName, "type", Map.of()));
            } catch (Exception e) {
                throw new RuntimeException("Failed on stream " + streamName, e);
            }
        }
        store.index.compact();
        Threads.sleep(10000);

        store.fromStream(StreamName.of(streamName))
                .forEachRemaining(System.out::println);
    }


    @Test
    public void fromAll_returns_all_items_when_store_is_reopened() {

        //given
        int size = 10000;
        String streamPrefix = "test-stream-";
        for (int i = 0; i < size; i++) {
            store.append(EventRecord.create(streamPrefix + i, "" + i, Map.of()));
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
            store.append(EventRecord.create(streamPrefix + i, "" + i, Map.of()));
        }

        store.close();

        try (IEventStore store = EventStore.open(directory)) {
            for (int i = 0; i < size; i++) {
                EventRecord event = store.get(StreamName.of(streamPrefix + i, START_VERSION));
                assertNotNull(event);
                assertEquals(START_VERSION, event.version);
                assertEquals(streamPrefix + i, event.stream);
            }
        }
    }

    @Test
    public void events_have_stream_and_version() {
        int numStreams = 1000;
        String streamPrefix = "stream-";
        for (int i = 0; i < numStreams; i++) {
            store.append(EventRecord.create(streamPrefix + i, String.valueOf(i), Map.of()));
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
            store.append(EventRecord.create(streamPrefix + i, String.valueOf(i), Map.of()));
        }

        //when
        for (int i = 0; i < numEvents; i++) {
            Iterator<EventRecord> events = store.fromStream(StreamName.parse(streamPrefix + i));

            assertTrue(events.hasNext());

            int size = 0;
            while (events.hasNext()) {
                EventRecord event = events.next();
                assertEquals(String.valueOf(i), event.type);
                assertEquals(String.valueOf(i), event.type);
                size++;
            }

            //then
            assertEquals(1, size);
        }
    }

    @Test
    public void fromStream_without_version_starts_from_zero() throws IOException {
        String stream = "stream-1";
        store.append(EventRecord.create(stream, "test", Map.of()));
        store.append(EventRecord.create(stream, "test", Map.of()));

        try (var it = store.fromStream(StreamName.parse(stream))) {
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
        store.append(EventRecord.create(stream1, "test", Map.of()));
        store.append(EventRecord.create(stream2, "test", Map.of()));

        try (var it = store.fromStreams(Set.of(StreamName.parse(stream1), StreamName.parse(stream2)))) {
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
            store.append(EventRecord.create("stream-" + i, "test", Map.of()));
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
                store.append(EventRecord.create(streamPrefix + stream, "type", Map.of()));
            }
        }

        StreamName eventToQuery = StreamName.parse("test-1");
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
    public void linkTo() {
        int size = 1000000;

        System.out.println("Creating entries");
        String streamName = "test-stream";
        for (int i = 0; i < size; i++) {
            store.append(EventRecord.create(streamName, "test", Map.of()));
        }

//        LogDump.dumpIndex(new File("index1.txt"), store);

        System.out.println("LinkTo 1");
        store.fromStream(StreamName.parse(streamName)).stream().forEach(e -> {
            String firstLetter = Arrays.toString(e.body).substring(0, 1);
            store.linkTo("letter-" + firstLetter, e);
        });

//        LogDump.dumpIndex(new File("index2.txt"), store);
        System.out.println("LinkTo 2");
        store.fromStream(StreamName.parse(streamName)).stream().forEach(e -> {
            String firstLetter = Arrays.toString(e.body).substring(0, 2);
            store.linkTo("letter-" + firstLetter, e);
        });

        System.out.println("LinkTo 3");
        store.fromStream(StreamName.parse(streamName)).stream().forEach(e -> {
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
            store.append(EventRecord.create(streamPrefix + i, "test", Map.of()));
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
            store.append(EventRecord.create(allStream, "test", Map.of()));
        }

        int numOtherIndexes = 5;

        assertEquals(size, store.fromStream(StreamName.parse(allStream)).stream().count());

        IntStream.range(0, numOtherIndexes).forEach(i -> {
            long start = System.currentTimeMillis();
            store.fromStream(StreamName.parse(allStream)).stream().forEach(e -> store.linkTo(String.valueOf(i), e));
            System.out.println("LinkTo " + size + " events to stream " + i + " in " + (System.currentTimeMillis() - start));
        });


        assertEquals(size, store.fromStream(StreamName.parse(allStream)).stream().count());

        for (int i = 0; i < numOtherIndexes; i++) {
            String streamName = String.valueOf(i);
            long foundSize = store.fromStream(StreamName.parse(streamName)).stream().count();
            assertEquals("Failed on iteration: " + i, size, foundSize);
        }
    }

    @Test
    public void fromStreamsPATTERN_returns_orderedEvents() {
        //given
        int numStreams = 1000;
        int numVersions = 50;
        String streamPrefix = "test-*";

        for (int stream = 0; stream < numStreams; stream++) {
            for (int version = 1; version <= numVersions; version++) {
                store.append(EventRecord.create(streamPrefix + stream, "type", Map.of()));
            }
        }

        //some other stream we don't care about
        for (int version = 1; version <= numVersions; version++) {
            store.append(EventRecord.create("someOtherStream", "type", Map.of()));
        }

        Iterator<EventRecord> eventStream = store.fromStreams(streamPrefix);

        assertTrue(eventStream.hasNext());

        int eventCounter = 0;
        while (eventStream.hasNext()) {
            EventRecord event = eventStream.next();
            eventCounter++;
            assertTrue(event.stream.startsWith(streamPrefix));
        }

        assertEquals(numStreams * numVersions, eventCounter);
    }

    @Test
    public void linkTo_events_are_resolved_on_get() {

        var original = "stream-1";
        var linkToStream = "stream-2";
        var created = store.append(EventRecord.create(original, "type", Map.of()));

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
        int size = 2000000; //flushThreshold must be less than this value
        for (int i = 0; i < size; i++) {
            store.append(EventRecord.create(stream, "type-1", Map.of()));
        }

        store.close();
        store = EventStore.open(directory);

        EventStoreIterator iterator = store.fromStream(StreamName.parse(stream));

        int total = 0;
        for (int i = 0; i < size; i++) {
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
        int size = 2000000; //flushThreshold must be less than this value
        for (int i = 0; i < size; i++) {
            store.append(EventRecord.create(stream, "type-1", Map.of()));
        }

        EventStoreIterator iterator = store.fromStream(StreamName.parse(SystemStreams.INDEX));
        if (!Iterators.await(iterator, 1000, 10000)) {
            fail("Did not receive any events");
        }
        List<EventRecord> indexEvents = iterator.stream().collect(Collectors.toList());
        assertFalse(indexEvents.isEmpty());
    }

    @Test
    public void all_streams_are_loaded_on_reopen() {

        int size = 1000010;
        var createdStreams = new HashSet<String>();
        for (int i = 0; i < size; i++) {
            var stream = "stream-" + i;
            createdStreams.add(stream);
            store.append(EventRecord.create(stream, "test", Map.of()));
        }

        Threads.sleep(20000);
        store.close();
        store = EventStore.open(directory);

        List<EventRecord> foundStreams = store.fromStream(StreamName.of(SystemStreams.STREAMS)).stream().collect(Collectors.toList());

        //stream also contains the stream definition itself
        assertTrue(foundStreams.size() >= createdStreams.size());

        int hits = 0;
        for (EventRecord streamRecord : foundStreams) {
            StreamMetadata streamMetadata = StreamCreated.from(streamRecord);
            if (createdStreams.contains(streamMetadata.name)) {
                hits++;
            }
        }
        assertEquals(createdStreams.size(), hits);
    }

    @Test
    public void compaction_removes_truncated_entries() {

        int size = 3000000;
        int truncateVersion = 400;
        String stream = "stream-123";
        for (int i = 0; i < size; i++) {
            store.append(EventRecord.create(stream, "test", Map.of()));
        }

        store.truncate(stream, truncateVersion);
        store.compact();
        Threads.sleep(TimeUnit.SECONDS.toMillis(5));

        long count = store.fromStream(StreamName.parse(stream)).stream().count();
        assertEquals(size - truncateVersion - 1, count);
    }

    private void testWith(int streams, int numVersionPerStream) {
        long start = System.currentTimeMillis();
        String streamPrefix = "test-stream-";

        for (int stream = 0; stream < streams; stream++) {
            String streamName = streamPrefix + stream;
            for (int version = 1; version <= numVersionPerStream; version++) {
                try {
                    store.append(EventRecord.create(streamName, "" + stream, Map.of()));
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
                try (Stream<EventRecord> events = store.fromStream(StreamName.parse(streamName)).stream()) {
                    assertEquals(numVersionPerStream, events.count());

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
