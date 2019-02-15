package io.joshworks.eventry.data;

import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.serializer.json.JsonSerializer;

import java.nio.ByteBuffer;

import static io.joshworks.eventry.StreamName.SYSTEM_PREFIX;

public class IndexFlushed {

    private static final Serializer<IndexFlushed> serializer = JsonSerializer.of(IndexFlushed.class);

    public final long timeTaken;
    public final int entries;

    public static final String TYPE = SYSTEM_PREFIX + "INDEX_FLUSHED";

    private IndexFlushed(long timeTaken, int entries) {
        this.timeTaken = timeTaken;
        this.entries = entries;
    }

    public static EventRecord create(long timeTaken, int entries) {
        var indexFlushed = new IndexFlushed(timeTaken, entries);
        var data = serializer.toBytes(indexFlushed);
        return EventRecord.create(SystemStreams.INDEX, TYPE, data.array());
    }

    public static IndexFlushed from(EventRecord record) {
        return serializer.fromBytes(ByteBuffer.wrap(record.body));
    }

}
