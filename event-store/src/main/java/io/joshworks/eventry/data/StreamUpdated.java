package io.joshworks.eventry.data;

import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.serializer.json.JsonSerializer;

import java.nio.ByteBuffer;

public class StreamUpdated {

    //serializing straight into a StreamMetadata
    private static final Serializer<StreamMetadata> serializer = JsonSerializer.of(StreamMetadata.class);

    public static final String TYPE = Constant.SYSTEM_PREFIX + "STREAM_UPDATED";

    public static EventRecord create(StreamMetadata metadata) {
        var data = serializer.toBytes(metadata);
        return EventRecord.create(SystemStreams.STREAMS, TYPE, data.array());
    }

    public static StreamMetadata from(EventRecord record) {
        return serializer.fromBytes(ByteBuffer.wrap(record.data));
    }

}
