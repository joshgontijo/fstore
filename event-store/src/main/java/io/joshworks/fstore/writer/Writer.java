package io.joshworks.fstore.writer;

import io.joshworks.fstore.data.IndexFlushed;
import io.joshworks.fstore.index.Index;
import io.joshworks.fstore.log.IEventLog;
import io.joshworks.fstore.stream.StreamException;
import io.joshworks.fstore.stream.StreamMetadata;
import io.joshworks.fstore.es.shared.EventRecord;
import io.joshworks.fstore.es.shared.streams.StreamHasher;
import io.joshworks.fstore.es.shared.streams.SystemStreams;

import java.util.concurrent.CompletableFuture;

import static io.joshworks.fstore.es.shared.EventId.NO_EXPECTED_VERSION;
import static io.joshworks.fstore.es.shared.EventId.NO_VERSION;

public class Writer {

    private final IEventLog eventLog;
    private final Index index;
    private EventWriter eventWriter;

    public Writer(IEventLog eventLog, Index index, EventWriter eventWriter) {
        this.eventLog = eventLog;
        this.index = index;
        this.eventWriter = eventWriter;
    }

    public EventRecord append(EventRecord event, int expectedVersion, StreamMetadata metadata, boolean newStream) {
        String streamName = metadata.name;
        long streamHash = metadata.hash;

        return appendInternal(event, expectedVersion, streamName, streamHash, newStream);
    }

    private EventRecord appendInternal(EventRecord event, int expectedVersion, String streamName, long streamHash, boolean newStream) {
        long eventStreamHash = StreamHasher.hash(event.stream);
        if (streamName.equals(event.stream) && streamHash != eventStreamHash) {
            throw new StreamException("Hash collision of stream: " + event.stream + " with existing name: " + streamName);
        }

        int currentVersion = newStream ? NO_VERSION : index.version(streamHash);
        if (!matchVersion(expectedVersion, currentVersion)) {
            throw new StreamException("Version mismatch for '" + streamName + "': expected: " + expectedVersion + " current :" + currentVersion);
        }
        int nextVersion = currentVersion + 1;

        long timestamp = System.currentTimeMillis();
        var record = new EventRecord(event.stream, event.type, nextVersion, timestamp, event.data, event.metadata);

        long position = eventLog.append(record);

        long start = System.currentTimeMillis();
        CompletableFuture<Void> memFlushed = index.add(streamHash, nextVersion, position);
        if (memFlushed != null) {
            memFlushed.thenRun(() -> {
                eventWriter.queue(() -> {
                    long end = System.currentTimeMillis();
                    var indexFlushedEvent = IndexFlushed.create(position, end - start);
                    appendInternal(indexFlushedEvent, NO_EXPECTED_VERSION, SystemStreams.INDEX, SystemStreams.INDEX_HASH, false);
                    return null;
                });
            });

        }
        return record;
    }


    private boolean matchVersion(int expected, int currentVersion) {
        return expected == NO_EXPECTED_VERSION || expected == currentVersion;
    }
}
