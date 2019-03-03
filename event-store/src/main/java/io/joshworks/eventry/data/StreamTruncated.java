package io.joshworks.eventry.data;

import io.joshworks.eventry.StreamName;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.serializer.json.JsonSerializer;

import java.nio.ByteBuffer;

public class StreamTruncated {

    //serializing straight into a StreamMetadata
    private static final Serializer<StreamTruncated> serializer = JsonSerializer.of(StreamTruncated.class);

    public final String stream;
    public final int versionAtDeletion;

    public static final String TYPE = StreamName.SYSTEM_PREFIX + "STREAM_TRUNCATED";

    private StreamTruncated(String stream, int versionAtDeletion) {
        this.stream = stream;
        this.versionAtDeletion = versionAtDeletion;
    }

    public static EventRecord create(String stream, int version) {
        var data = serializer.toBytes(new StreamTruncated(stream, version));
        return EventRecord.create(SystemStreams.STREAMS, TYPE, data.array());
    }

    public static StreamTruncated from(EventRecord record) {
        return serializer.fromBytes(ByteBuffer.wrap(record.body));
    }

}
