package io.joshworks.fstore.data;

import io.joshworks.fstore.es.shared.EventId;
import io.joshworks.fstore.es.shared.EventRecord;
import io.joshworks.fstore.es.shared.streams.SystemStreams;
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
        return JsonSerializer.fromJson(record.data, ProjectionDeleted.class);
    }
}
