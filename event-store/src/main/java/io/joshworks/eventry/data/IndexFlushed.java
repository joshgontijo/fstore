package io.joshworks.eventry.data;

import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.serializer.json.JsonSerializer;

import java.nio.ByteBuffer;

import static io.joshworks.eventry.StreamName.SYSTEM_PREFIX;

public class IndexFlushed {

    private static final Serializer<IndexFlushed> serializer = JsonSerializer.of(IndexFlushed.class);

    public final long logPosition;
    public final long timeTaken;

    public static final String TYPE = SYSTEM_PREFIX + "INDEX_FLUSHED";

    private IndexFlushed(long logPosition, long timeTaken) {
        this.logPosition = logPosition;
        this.timeTaken = timeTaken;
    }

    public static EventRecord create(long logPosition, long timeTaken) {
        var indexFlushed = new IndexFlushed(logPosition, timeTaken);
        var data = serializer.toBytes(indexFlushed);
        return EventRecord.create(SystemStreams.INDEX, TYPE, data.array());
    }

    public static IndexFlushed from(EventRecord record) {
        return serializer.fromBytes(ByteBuffer.wrap(record.body));
    }

}
