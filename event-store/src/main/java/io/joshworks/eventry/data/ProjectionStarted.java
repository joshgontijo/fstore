package io.joshworks.eventry.data;

import io.joshworks.eventry.StreamName;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.serializer.json.JsonSerializer;

public class ProjectionStarted {

    public final String name;

    public static final String TYPE = StreamName.SYSTEM_PREFIX + "PROJECTION_RUN_STARTED";

    private ProjectionStarted(String name) {
        this.name = name;
    }

    public static EventRecord create(String name) {
        var data = JsonSerializer.toBytes(new ProjectionStarted(name));
        return EventRecord.create(SystemStreams.PROJECTIONS, TYPE, data);
    }

    public static ProjectionStarted from(EventRecord record) {
        return JsonSerializer.fromBytes(record.body, ProjectionStarted.class);
    }

}
