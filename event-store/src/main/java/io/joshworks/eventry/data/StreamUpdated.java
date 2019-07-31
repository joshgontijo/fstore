package io.joshworks.eventry.data;

import io.joshworks.eventry.EventId;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.fstore.serializer.json.JsonSerializer;

public class StreamUpdated {

    public static final String TYPE = EventId.SYSTEM_PREFIX + "STREAM_UPDATED";

    public static EventRecord create(StreamMetadata metadata) {
        var data = JsonSerializer.toBytes(metadata);
        return EventRecord.create(SystemStreams.STREAMS, TYPE, data);
    }

    public static StreamMetadata from(EventRecord record) {
        return JsonSerializer.fromBytes(record.body, StreamMetadata.class);
    }

}
