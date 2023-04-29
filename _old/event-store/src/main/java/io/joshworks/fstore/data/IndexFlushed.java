package io.joshworks.fstore.data;

import io.joshworks.fstore.es.shared.EventRecord;
import io.joshworks.fstore.es.shared.streams.SystemStreams;
import io.joshworks.fstore.serializer.json.JsonSerializer;

import static io.joshworks.fstore.es.shared.EventId.SYSTEM_PREFIX;

public class IndexFlushed {

    public static final String TYPE = SYSTEM_PREFIX + "INDEX_FLUSHED";
    public final long logPosition;
    public final long timeTaken;

    private IndexFlushed(long logPosition, long timeTaken) {
        this.logPosition = logPosition;
        this.timeTaken = timeTaken;
    }

    public static EventRecord create(long logPosition, long timeTaken) {
        var indexFlushed = new IndexFlushed(logPosition, timeTaken);
        var data = JsonSerializer.toBytes(indexFlushed);
        return EventRecord.create(SystemStreams.INDEX, TYPE, data);
    }

    public static IndexFlushed from(EventRecord record) {
        return JsonSerializer.fromJson(record.data, IndexFlushed.class);
    }

}
