package io.joshworks.eventry.writer;

import io.joshworks.eventry.data.IndexFlushed;
import io.joshworks.eventry.index.Index;
import io.joshworks.eventry.log.IEventLog;
import io.joshworks.eventry.stream.StreamException;
import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.fstore.es.shared.EventRecord;
import io.joshworks.fstore.es.shared.streams.StreamHasher;
import io.joshworks.fstore.es.shared.streams.SystemStreams;

import static io.joshworks.fstore.es.shared.EventId.NO_EXPECTED_VERSION;

public class Writer {

    private final IEventLog eventLog;
    private final Index index;

    Writer(IEventLog eventLog, Index index) {
        this.eventLog = eventLog;
        this.index = index;
    }

    public EventRecord append(EventRecord event, int expectedVersion, StreamMetadata metadata) {
        String streamName = metadata.name;
        long streamHash = metadata.hash;

        return appendInternal(event, expectedVersion, streamName, streamHash);
    }

    private EventRecord appendInternal(EventRecord event, int expectedVersion, String streamName, long streamHash) {
        long eventStreamHash = StreamHasher.hash(event.stream);
        if (streamName.equals(event.stream) && streamHash != eventStreamHash) {
            throw new StreamException("Hash collision of stream: " + event.stream + " with existing name: " + streamName);
        }

        int currentVersion = index.version(streamHash);
        if (!matchVersion(expectedVersion, currentVersion)) {
            throw new StreamException("Version mismatch for '" + streamName + "': expected: " + expectedVersion + " current :" + currentVersion);
        }
        int nextVersion = currentVersion + 1;

        long timestamp = System.currentTimeMillis();
        var record = new EventRecord(event.stream, event.type, nextVersion, timestamp, event.data, event.metadata);

        long position = eventLog.append(record);

        long start = System.currentTimeMillis();
        boolean memFlushed = index.add(eventStreamHash, nextVersion, position);
        if (memFlushed) {
            long end = System.currentTimeMillis();
            var indexFlushedEvent = IndexFlushed.create(position, end - start);
            appendInternal(indexFlushedEvent, NO_EXPECTED_VERSION, SystemStreams.INDEX, SystemStreams.INDEX_HASH);
        }
        return record;
    }


    private boolean matchVersion(int expected, int currentVersion) {
        return expected == NO_EXPECTED_VERSION || expected == currentVersion;
    }
}
