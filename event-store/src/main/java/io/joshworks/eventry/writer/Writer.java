package io.joshworks.eventry.writer;

import io.joshworks.eventry.index.TableIndex;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.log.IEventLog;
import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.eventry.stream.Streams;

public class Writer {

    private final Streams streams;
    private final IEventLog eventLog;
    private final TableIndex index;

    Writer(Streams streams, IEventLog eventLog, TableIndex index) {
        this.streams = streams;
        this.eventLog = eventLog;
        this.index = index;
    }

    public EventRecord append(EventRecord event, int expectedVersion, StreamMetadata metadata) {
        long streamHash = event.streamName().hash();

        if (metadata.name.equals(event.stream) && metadata.hash != streamHash) {
            //TODO improve ??
            throw new IllegalStateException("Hash collision of closeableStream: " + event.stream + " with existing name: " + metadata.name);
        }

        int version = streams.tryIncrementVersion(metadata, expectedVersion);
        var record = new EventRecord(event.stream, event.type, version, System.currentTimeMillis(), event.body, event.metadata);

        long position = eventLog.append(record);
        index.add(streamHash, version, position);
        return record;
    }
}
