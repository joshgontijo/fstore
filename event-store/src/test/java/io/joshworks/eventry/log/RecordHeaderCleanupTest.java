package io.joshworks.eventry.log;

import io.joshworks.eventry.data.LinkTo;
import io.joshworks.eventry.data.SystemStreams;
import io.joshworks.eventry.index.IndexEntry;
import io.joshworks.eventry.stream.Streams;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.Iterators;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.PollingSubscriber;
import io.joshworks.fstore.log.TimeoutReader;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.Marker;
import io.joshworks.fstore.log.segment.SegmentState;
import io.joshworks.fstore.log.segment.header.Type;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static io.joshworks.eventry.stream.StreamMetadata.NO_MAX_AGE;
import static io.joshworks.eventry.stream.StreamMetadata.NO_MAX_COUNT;
import static org.junit.Assert.assertEquals;

public class RecordHeaderCleanupTest {

    private RecordCleanup cleanup;
    private Streams streams;

    @Before
    public void setUp() {
        streams = new Streams(100000, hash -> 0);
        cleanup = new RecordCleanup(streams);
    }

    @Test(expected = IllegalArgumentException.class)
    public void cleanup_can_only_be_executed_in_a_single_segment() {

        var source = new InMemorySegment();
        var output = new InMemorySegment();

        cleanup.merge(List.of(source, source), output);
    }

    @Test
    public void system_events_are_always_written_to_new_segment() {

        var stream = SystemStreams.STREAMS;

        streams.create(stream, 1, 1);

        var source = new InMemorySegment();
        appendTo(source, systemRecord());
        appendTo(source, systemRecord());
        appendTo(source, systemRecord());

        var output = new InMemorySegment();
        cleanup.merge(List.of(source), output);

        assertEquals(3, output.records.size());

    }

    @Test
    public void obsolete_entries_are_removed() {

        var stream = "stream-1";
        var maxCount = 2;

        streams.create(stream, NO_MAX_AGE, maxCount);

        var source = new InMemorySegment();
        appendTo(source, recordOf(stream, 0, 0));
        appendTo(source, recordOf(stream, 1, 0));
        appendTo(source, recordOf(stream, 2, 0));


        var output = new InMemorySegment();
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

        var source = new InMemorySegment();
        appendTo(source, recordOf(stream, 0, now - maxAge - 1));
        appendTo(source, recordOf(stream, 1, now + TimeUnit.MINUTES.toMillis(1)));
        appendTo(source, recordOf(stream, 2, now + TimeUnit.MINUTES.toMillis(1)));

        var output = new InMemorySegment();
        cleanup.merge(List.of(source), output);

        assertEquals(2, output.records.size());
        assertEquals(1, output.records.get(0).version);
        assertEquals(2, output.records.get(1).version);

    }


    private EventRecord recordOf(String stream, int version, long timestamp) {
        return new EventRecord(stream, "type", version, timestamp, new byte[0], new byte[0]);
    }

    private EventRecord systemRecord() {
        return new EventRecord(SystemStreams.STREAMS, LinkTo.TYPE,  0, 0, new byte[0], new byte[0]);
    }

    private void appendTo(Log<EventRecord> segment, EventRecord record) {
        segment.append(record);
        streams.tryIncrementVersion(streams.hashOf(record.stream), IndexEntry.NO_VERSION);
    }


    private class InMemorySegment implements Log<EventRecord> {

        public final List<EventRecord> records = new ArrayList<>();

        @Override
        public String name() {
            return "mem-segment";
        }

        @Override
        public Stream<EventRecord> stream(Direction direction) {
            return records.stream();
        }

        @Override
        public LogIterator<EventRecord> iterator(long position, Direction direction) {
            return null;
        }

        @Override
        public LogIterator<EventRecord> iterator(Direction direction) {
            return Iterators.of(records);
        }

        @Override
        public long position() {
            return 0;
        }

        @Override
        public Marker marker() {
            return null;
        }

        @Override
        public EventRecord get(long position) {
            return null;
        }

        @Override
        public PollingSubscriber<EventRecord> poller(long position) {
            return null;
        }

        @Override
        public PollingSubscriber<EventRecord> poller() {
            return null;
        }

        @Override
        public long fileSize() {
            return 0;
        }

        @Override
        public long logicalSize() {
            return 0;
        }

        @Override
        public Set<TimeoutReader> readers() {
            return null;
        }

        @Override
        public SegmentState rebuildState(long lastKnownPosition) {
            return null;
        }

        @Override
        public void delete() {

        }

        @Override
        public void roll(int level) {

        }

        @Override
        public void roll(int level, ByteBuffer footer) {

        }

        @Override
        public ByteBuffer readFooter() {
            return null;
        }

        @Override
        public boolean readOnly() {
            return false;
        }

        @Override
        public long entries() {
            return records.size();
        }

        @Override
        public int level() {
            return 0;
        }

        @Override
        public long created() {
            return 0;
        }

        @Override
        public Type type() {
            return null;
        }

        @Override
        public long append(EventRecord data) {
            int size = records.size();
            records.add(data);
            return size;
        }

        @Override
        public void close() {

        }

        @Override
        public void flush() {

        }
    }

}



