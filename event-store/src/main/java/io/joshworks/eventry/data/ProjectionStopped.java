package io.joshworks.eventry.data;

import io.joshworks.fstore.es.shared.EventId;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.es.shared.streams.SystemStreams;
import io.joshworks.fstore.serializer.json.JsonSerializer;

public class ProjectionStopped {

    public final String name;
    public final String reason;
    public final long processedItems;

    public static final String TYPE = EventId.SYSTEM_PREFIX + "PROJECTION_RUN_STOPPED";

    private ProjectionStopped(String name, String reason, long processedItems) {
        this.name = name;
        this.reason = reason;
        this.processedItems = processedItems;
    }

    public static EventRecord create(String name, String reason, long processedItems) {
        var data = JsonSerializer.toBytes(new ProjectionStopped(name, reason, processedItems));
        return EventRecord.create(SystemStreams.PROJECTIONS, TYPE, data);
    }

    public static ProjectionStopped from(EventRecord record) {
        return JsonSerializer.fromBytes(record.body, ProjectionStopped.class);
    }

}
