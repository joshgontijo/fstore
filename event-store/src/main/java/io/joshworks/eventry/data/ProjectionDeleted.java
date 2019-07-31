package io.joshworks.eventry.data;

import io.joshworks.eventry.EventId;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.serializer.json.JsonSerializer;

public class ProjectionDeleted {

    public final String name;

    public static final String TYPE = EventId.SYSTEM_PREFIX + "PROJECTION_DELETED";

    private ProjectionDeleted(String name) {
        this.name = name;
    }

    public static EventRecord create(String name) {
        var data = JsonSerializer.toBytes(new ProjectionDeleted(name));
        return EventRecord.create(SystemStreams.PROJECTIONS, TYPE, data);
    }

    public static ProjectionDeleted from(EventRecord record) {
        return JsonSerializer.fromBytes(record.body, ProjectionDeleted.class);
    }
}
