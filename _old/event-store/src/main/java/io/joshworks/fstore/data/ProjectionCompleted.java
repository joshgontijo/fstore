package io.joshworks.fstore.data;

import io.joshworks.fstore.es.shared.EventId;
import io.joshworks.fstore.es.shared.EventRecord;
import io.joshworks.fstore.es.shared.streams.SystemStreams;
import io.joshworks.fstore.serializer.json.JsonSerializer;

public class ProjectionCompleted {

    public static final String TYPE = EventId.SYSTEM_PREFIX + "PROJECTION_RUN_COMPLETED";
    public final String id;
    public final long processedItems;

    private ProjectionCompleted(String id, long processedItems) {
        this.id = id;
        this.processedItems = processedItems;
    }

    public static EventRecord create(String id, long processedItems) {
        var data = JsonSerializer.toBytes(new ProjectionCompleted(id, processedItems));
        return EventRecord.create(SystemStreams.PROJECTIONS, TYPE, data);
    }

    public static ProjectionCompleted from(EventRecord record) {
        return JsonSerializer.fromJson(record.data, ProjectionCompleted.class);
    }

}
