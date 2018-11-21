package io.joshworks.eventry.data;

import io.joshworks.eventry.StreamName;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.serializer.json.JsonSerializer;

import java.nio.ByteBuffer;

public class ProjectionDeleted {

    public final String name;

    public static final String TYPE = StreamName.SYSTEM_PREFIX + "PROJECTION_DELETED";
    private static final Serializer<ProjectionDeleted> serializer = JsonSerializer.of(ProjectionDeleted.class);

    public ProjectionDeleted(String name) {
        this.name = name;
    }

    public static EventRecord create(String name) {
        var data = serializer.toBytes(new ProjectionDeleted(name));
        return EventRecord.create(SystemStreams.PROJECTIONS, TYPE, data.array());
    }

    public static ProjectionDeleted from(EventRecord record) {
        return serializer.fromBytes(ByteBuffer.wrap(record.body));
    }
}
