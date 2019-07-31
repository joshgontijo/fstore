package io.joshworks.eventry.data;

import io.joshworks.eventry.EventId;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.serializer.json.JsonSerializer;

public class ProjectionCompleted {

    public final String id;
    public final long processedItems;

    public static final String TYPE = EventId.SYSTEM_PREFIX + "PROJECTION_RUN_COMPLETED";

    private ProjectionCompleted(String id, long processedItems) {
        this.id = id;
        this.processedItems = processedItems;
    }

    public static EventRecord create(String id, long processedItems) {
        var data = JsonSerializer.toBytes(new ProjectionCompleted(id, processedItems));
        return EventRecord.create(SystemStreams.PROJECTIONS, TYPE, data);
    }

    public static ProjectionCompleted from(EventRecord record) {
        return JsonSerializer.fromBytes(record.body, ProjectionCompleted.class);
    }

}
