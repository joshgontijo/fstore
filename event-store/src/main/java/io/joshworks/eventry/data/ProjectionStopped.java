package io.joshworks.eventry.data;

import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.serializer.json.JsonSerializer;

import java.nio.ByteBuffer;

public class ProjectionStopped {

    public final String name;
    public final String reason;
    public final long processedItems;

    public static final String TYPE = Constant.SYSTEM_PREFIX + "PROJECTION_RUN_STOPPED";
    private static final JsonSerializer<ProjectionStopped> serializer = JsonSerializer.of(ProjectionStopped.class);

    public ProjectionStopped(String name, String reason, long processedItems) {
        this.name = name;
        this.reason = reason;
        this.processedItems = processedItems;
    }

    public static EventRecord create(String name, String reason, long processedItems) {
        var data = serializer.toBytes(new ProjectionStopped(name, reason, processedItems));
        return EventRecord.create(SystemStreams.PROJECTIONS, TYPE, data.array());
    }

    public static ProjectionStopped from(EventRecord record) {
        return serializer.fromBytes(ByteBuffer.wrap(record.data));
    }

}
