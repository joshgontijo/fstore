package io.joshworks.eventry.writer;

import io.joshworks.eventry.index.TableIndex;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.log.IEventLog;
import io.joshworks.eventry.stream.StreamException;
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
        long streamHash = event.hash();

        if (metadata.name.equals(event.stream) && metadata.hash != streamHash) {
            throw new StreamException("Hash collision of stream: " + event.stream + " with existing name: " + metadata.name);
        }

        int version = streams.tryIncrementVersion(metadata, expectedVersion);
        long timestamp = System.currentTimeMillis();

        var record = new EventRecord(event.stream, event.type, version, timestamp, event.body, event.metadata);

        long position = eventLog.append(record);

        if (index.add(streamHash, version, position)) {
            index.flushAsync(eventLog.position());
        }
        return record;
    }
}
