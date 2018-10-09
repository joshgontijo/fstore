package io.joshworks.eventry.data;

import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.serializer.json.JsonSerializer;

import java.nio.ByteBuffer;

public class ProjectionCompleted {

    public final String id;
    public final long processedItems;

    public static final String TYPE = Constant.SYSTEM_PREFIX + "PROJECTION_RUN_COMPLETED";
    private static final JsonSerializer<ProjectionCompleted> serializer = JsonSerializer.of(ProjectionCompleted.class);

    public ProjectionCompleted(String id, long processedItems) {
        this.id = id;
        this.processedItems = processedItems;
    }

    public static EventRecord create(String id, long processedItems) {
        var data = serializer.toBytes(new ProjectionCompleted(id, processedItems));
        return EventRecord.create(SystemStreams.PROJECTIONS, TYPE, data.array());
    }

    public static ProjectionCompleted from(EventRecord record) {
        return serializer.fromBytes(ByteBuffer.wrap(record.data));
    }

}
