package io.joshworks.eventry.data;

import io.joshworks.eventry.StreamName;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.serializer.json.JsonSerializer;

import java.nio.ByteBuffer;

public class ProjectionResumed {

    public final String name;
    public final String reason;
    public final String streamName;
    public final int streamVersion;
    public final long processedItems;

    public static final String TYPE = StreamName.SYSTEM_PREFIX + "PROJECTION_RUN_FAILED";

    private ProjectionResumed(String name, String reason, long processedItems, String streamName, int streamVersion) {
        this.name = name;
        this.reason = reason;
        this.processedItems = processedItems;
        this.streamName = streamName;
        this.streamVersion = streamVersion;
    }

    public static EventRecord create(String name, String reason, long processedItems, String streamName, int streamVersion) {
        var data = JsonSerializer.toBytes(new ProjectionResumed(name, reason, processedItems, streamName, streamVersion));
        return EventRecord.create(SystemStreams.PROJECTIONS, TYPE, data);
    }

    public static ProjectionResumed from(EventRecord record) {
        return JsonSerializer.fromBytes(record.body, ProjectionResumed.class);
    }

}
