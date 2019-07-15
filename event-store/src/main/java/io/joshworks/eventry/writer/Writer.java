package io.joshworks.eventry.writer;

import io.joshworks.eventry.data.IndexFlushed;
import io.joshworks.eventry.data.StreamCreated;
import io.joshworks.eventry.data.SystemStreams;
import io.joshworks.eventry.index.Index;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.log.IEventLog;
import io.joshworks.eventry.stream.StreamException;
import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.eventry.stream.Streams;

import static io.joshworks.eventry.log.EventRecord.NO_EXPECTED_VERSION;

public class Writer {

    private final Streams streams;
    private final IEventLog eventLog;
    private final Index index;

    Writer(Streams streams, IEventLog eventLog, Index index) {
        this.streams = streams;
        this.eventLog = eventLog;
        this.index = index;
    }

    public EventRecord append(EventRecord event, int expectedVersion, StreamMetadata metadata) {
        long streamHash = event.hash();

        if (metadata.name.equals(event.stream) && metadata.hash != streamHash) {
            throw new StreamException("Hash collision of stream: " + event.stream + " with existing name: " + metadata.name);
        }

        int version = streams.tryIncrementVersion(metadata, expectedVersion);
        long timestamp = System.currentTimeMillis();

        var record = new EventRecord(event.stream, event.type, version, timestamp, event.body, event.metadata);

        long position = eventLog.append(record);

        long start = System.currentTimeMillis();
        boolean memFlushed = index.add(streamHash, version, position);
        if (memFlushed) {
            long end = System.currentTimeMillis();
            var indexFlushedEvent = IndexFlushed.create(position, end - start);
            StreamMetadata flushEventMetadata = getOrCreateStream(indexFlushedEvent.stream);
            append(indexFlushedEvent, NO_EXPECTED_VERSION, flushEventMetadata);
        }
        return record;
    }

    private StreamMetadata getOrCreateStream(String stream) {
        return streams.createIfAbsent(stream, created -> {
            EventRecord eventRecord = StreamCreated.create(created);
            StreamMetadata metadata = streams.get(SystemStreams.STREAMS).get();
            append(eventRecord, NO_EXPECTED_VERSION, metadata);
        });
    }
}
