package io.joshworks.eventry.data;

import io.joshworks.eventry.StreamName;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.fstore.serializer.json.JsonSerializer;

public class StreamCreated {


    public static final String TYPE = StreamName.SYSTEM_PREFIX + "STREAM_CREATED";

    public static EventRecord create(StreamMetadata metadata) {
        //serializing straight into a StreamMetadata
        var data = JsonSerializer.toBytes(metadata);
        return EventRecord.create(SystemStreams.STREAMS, TYPE, data);
    }

    public static StreamMetadata from(EventRecord record) {
        return JsonSerializer.fromBytes(record.body, StreamMetadata.class);
    }

}
