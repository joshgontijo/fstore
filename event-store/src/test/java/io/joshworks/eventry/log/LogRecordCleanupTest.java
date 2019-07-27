package io.joshworks.eventry.log;

import io.joshworks.eventry.InMemorySegment;
import io.joshworks.eventry.StreamName;
import io.joshworks.eventry.data.LinkTo;
import io.joshworks.eventry.data.StreamCreated;
import io.joshworks.eventry.data.SystemStreams;
import io.joshworks.eventry.index.Index;
import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.eventry.stream.Streams;
import io.joshworks.eventry.utils.StringUtils;
import io.joshworks.fstore.core.cache.Cache;
import io.joshworks.fstore.core.util.FileUtils;
import io.joshworks.fstore.log.segment.Log;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.joshworks.eventry.stream.StreamMetadata.NO_MAX_AGE;
import static io.joshworks.eventry.stream.StreamMetadata.NO_MAX_COUNT;
import static io.joshworks.eventry.stream.StreamMetadata.NO_TRUNCATE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LogRecordCleanupTest {

    private static final int STREAMS_FLUSH_THRESHOLD = 1000;
    private RecordCleanup cleanup;
    private Streams streams;

    private File dummyFolder;
    private Index index;

    @Before
    public void setUp() {
        dummyFolder = FileUtils.testFolder();
        streams = new Streams(dummyFolder, STREAMS_FLUSH_THRESHOLD, Cache.noCache());
        index = new Index(dummyFolder, 10, Cache.noCache(), LogRecordCleanupTest::dummyMetadata);
        cleanup = new RecordCleanup(streams, index);
    }

    @After
    public void tearDown() {
        streams.close();
        index.close();
        FileUtils.tryDelete(dummyFolder);
    }

    private static StreamMetadata dummyMetadata(long stream) {
        return new StreamMetadata(String.valueOf(stream), stream, 0, NO_MAX_AGE, NO_MAX_COUNT, NO_TRUNCATE, new HashMap<>(), new HashMap<>(), StreamMetadata.STREAM_ACTIVE);
    }

    @Test(expected = IllegalArgumentException.class)
    public void cleanup_can_only_be_executed_in_a_single_segment() {

        var source = new InMemorySegment<EventRecord>();
        var output = new InMemorySegment<EventRecord>();

        cleanup.merge(List.of(source, source), output);
    }

    @Test
    public void system_events_are_always_written_to_new_segment() {

        var stream = SystemStreams.STREAMS;

        streams.create(stream);

        var source = new InMemorySegment<EventRecord>();
        appendTo(source, systemRecord());
        appendTo(source, systemRecord());
        appendTo(source, systemRecord());

        var output = new InMemorySegment<EventRecord>();
        cleanup.merge(List.of(source), output);

        assertEquals(3, output.records.size());

    }

    @Test
    public void obsolete_entries_are_removed() {

        var stream = "stream-1";
        var maxCount = 2;

        streams.create(stream, NO_MAX_AGE, maxCount);

        var source = new InMemorySegment<EventRecord>();
        appendTo(source, recordOf(stream, 0, 0));
        appendTo(source, recordOf(stream, 1, 0));
        appendTo(source, recordOf(stream, 2, 0));


        var output = new InMemorySegment<EventRecord>();
        cleanup.merge(List.of(source), output);

        assertEquals(2, output.records.size());
        assertEquals(1, output.records.get(0).version);
        assertEquals(2, output.records.get(1).version);

    }

    @Test
    public void expired_entries_are_removed() {

        var stream = "stream-1";
        var maxAge = 10;
        var now = System.currentTimeMillis();
        streams.create(stream, maxAge, NO_MAX_COUNT);

        var source = new InMemorySegment<EventRecord>();
        appendTo(source, recordOf(stream, 0, now - maxAge - 1));
        appendTo(source, recordOf(stream, 1, now + TimeUnit.MINUTES.toMillis(1)));
        appendTo(source, recordOf(stream, 2, now + TimeUnit.MINUTES.toMillis(1)));

        var output = new InMemorySegment<EventRecord>();
        cleanup.merge(List.of(source), output);

        assertEquals(2, output.records.size());
        assertEquals(1, output.records.get(0).version);
        assertEquals(2, output.records.get(1).version);
    }

    @Test
    public void obsolete_linkTo_entries_are_removed() {

        var originalStream = "original-stream";
        var derivedStream = "derived-stream";
        var maxCount = 1;

        streams.create(originalStream);
        streams.create(derivedStream, NO_MAX_AGE, maxCount);

        var source = new InMemorySegment<EventRecord>();

        EventRecord ev1 = recordOf(originalStream, 0, 0);
        EventRecord ev2 = recordOf(originalStream, 1, 0);

        appendTo(source, ev1);
        appendTo(source, ev2);

        appendTo(source, linkTo(derivedStream, ev1, 0, 0));
        appendTo(source, linkTo(derivedStream, ev2, 1, 0));

        var output = new InMemorySegment<EventRecord>();
        cleanup.merge(List.of(source), output);

        assertEquals(3, output.records.size());

        EventRecord first = output.records.get(0);
        assertEquals(originalStream, first.stream);
        assertEquals(0, first.version);

        EventRecord second = output.records.get(1);
        assertEquals(originalStream, second.stream);
        assertEquals(1, second.version);

        //derived stream should have only one
        EventRecord third = output.records.get(2);
        assertEquals(derivedStream, third.stream);
        assertEquals(1, third.version);

    }

    @Test
    public void expired_linkTo_entries_are_removed() {

        var originalStream = "original-stream";
        var derivedStream = "derived-stream";

        var maxAge = 10;
        var now = System.currentTimeMillis();

        streams.create(originalStream);
        streams.create(derivedStream, maxAge, NO_MAX_COUNT);

        var source = new InMemorySegment<EventRecord>();

        long expiredTs = now - maxAge - 1;
        long validTs = now + TimeUnit.HOURS.toMillis(1);

        EventRecord ev1 = recordOf(originalStream, 0, validTs);
        EventRecord ev2 = recordOf(originalStream, 1, validTs);

        appendTo(source, ev1);
        appendTo(source, ev2);

        appendTo(source, linkTo(derivedStream, ev1, 0, expiredTs));
        appendTo(source, linkTo(derivedStream, ev2, 1, validTs));

        var output = new InMemorySegment<EventRecord>();
        cleanup.merge(List.of(source), output);

        assertEquals(3, output.records.size());

        EventRecord first = output.records.get(0);
        assertEquals(originalStream, first.stream);
        assertEquals(0, first.version);

        EventRecord second = output.records.get(1);
        assertEquals(originalStream, second.stream);
        assertEquals(1, second.version);

        //derived stream should have only one
        EventRecord third = output.records.get(2);
        assertEquals(derivedStream, third.stream);
        assertEquals(1, third.version);
    }

    @Test
    public void truncated_events_are_removed() {

        var stream = "source-stream";
        StreamMetadata srcMetadata = streams.create(stream);

        var source = new InMemorySegment<EventRecord>();
        EventRecord ev1 = recordOf(stream, 0, 0);
        EventRecord ev2 = recordOf(stream, 1, 0);
        EventRecord ev3 = recordOf(stream, 2, 0);

        //original events
        appendTo(source, ev1);
        appendTo(source, ev2);
        appendTo(source, ev3);

        streams.truncate(srcMetadata, 2, 1);

        var output = new InMemorySegment<EventRecord>();
        cleanup.merge(List.of(source), output);

        assertEquals(1, output.records.size());

        EventRecord record = output.records.get(0);
        assertEquals(stream, record.stream);
        assertEquals(2, record.version);
    }

    @Test
    public void dead_linkTo_references_are_removed_when_original_is_truncated() {

        var src = "source-stream";
        var tgt = "linkTo-stream";
        StreamMetadata srcMetadata = streams.create(src);
        streams.create(tgt);

        var source = new InMemorySegment<EventRecord>();
        EventRecord ev1 = recordOf(src, 0, 0);
        EventRecord ev2 = recordOf(src, 1, 0);
        EventRecord ev3 = recordOf(src, 2, 0);

        //original events
        appendTo(source, ev1);
        appendTo(source, ev2);
        appendTo(source, ev3);

        //linkTo events
        appendTo(source, linkTo(tgt, ev1, 0, 0));
        appendTo(source, linkTo(tgt, ev2, 1, 0));
        appendTo(source, linkTo(tgt, ev3, 2, 0));

        streams.truncate(srcMetadata, 2, 1);


        var output = new InMemorySegment<EventRecord>();
        cleanup.merge(List.of(source), output);

        assertEquals(2, output.records.size());

        assertEquals(src, output.records.get(0).stream);
        assertEquals(2, output.records.get(0).version);

        EventRecord linkToRecord = output.records.get(1);
        assertTrue(linkToRecord.isLinkToEvent());
        assertEquals(tgt, linkToRecord.stream);
        assertEquals(2, linkToRecord.version);
    }

    @Test
    public void dead_linkTo_references_are_removed_when_original_entry_expires() {

        var srcStream = "src-stream";
        var derivedStream = "derived-stream";
        var maxAge = 10;
        var now = System.currentTimeMillis();
        streams.create(srcStream, maxAge, NO_MAX_COUNT);
        streams.create(derivedStream); //no maxAge / maxCount for derived stream

        long ts1 = now - maxAge - 1;
        long ts2 = now + TimeUnit.HOURS.toMillis(1);

        EventRecord ev1 = recordOf(srcStream, 0, ts1);
        EventRecord ev2 = recordOf(srcStream, 1, ts2);

        var source = new InMemorySegment<EventRecord>();
        appendTo(source, ev1);
        appendTo(source, ev2);

        //assuming the derived stream events are created the same timestamp as the source
        appendTo(source, linkTo(derivedStream, ev1, 0, ts1));
        appendTo(source, linkTo(derivedStream, ev2, 1, ts2));

        var output = new InMemorySegment<EventRecord>();
        cleanup.merge(List.of(source), output);

        assertEquals(2, output.records.size()); //2 main stream + 2 derived
        assertEquals(derivedStream, output.records.get(1).stream);
        assertEquals(1, output.records.get(1).version);
    }

    @Test
    public void dead_linkTo_references_are_removed_when_original_entry_becomes_obsolete() {

        var srcStream = "stream-1";
        var derivedStream = "derived-stream";
        var maxCount = 1;

        streams.create(srcStream, NO_MAX_AGE, maxCount);
        streams.create(derivedStream);

        EventRecord ev1 = recordOf(srcStream, 0, 0);
        EventRecord ev2 = recordOf(srcStream, 1, 0);

        var source = new InMemorySegment<EventRecord>();
        appendTo(source, ev1);
        appendTo(source, ev2);

        appendTo(source, linkTo(derivedStream, ev1, 0, 0));
        appendTo(source, linkTo(derivedStream, ev2, 1, 0));

        var output = new InMemorySegment<EventRecord>();
        cleanup.merge(List.of(source), output);

        assertEquals(2, output.records.size());

        EventRecord record = output.records.get(1);
        assertEquals(derivedStream, record.stream);
        assertEquals(1, record.version);
    }

    private EventRecord recordOf(String stream, int version, long timestamp) {
        return new EventRecord(stream, "type", version, timestamp, new byte[0], new byte[0]);
    }

    private EventRecord linkTo(String stream, EventRecord record, int version, long timestamp) {
        StreamName streamName = StreamName.from(record);
        return new EventRecord(stream, LinkTo.TYPE, version, timestamp, StringUtils.toUtf8Bytes(streamName.toString()), new byte[0]);
    }

    private EventRecord systemRecord() {
        return new EventRecord(SystemStreams.STREAMS, StreamCreated.TYPE, 0, 0, new byte[0], new byte[0]);
    }

    private void appendTo(Log<EventRecord> segment, EventRecord record) {
        long pos = segment.append(record);
        index.add(record.hash(), record.version, pos);
    }


}



