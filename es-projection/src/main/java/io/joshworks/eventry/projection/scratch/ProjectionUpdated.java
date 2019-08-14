package io.joshworks.eventry.projection.scratch;

import io.joshworks.eventry.projection.Projection;
import io.joshworks.fstore.es.shared.EventId;
import io.joshworks.fstore.es.shared.EventRecord;
import io.joshworks.fstore.es.shared.streams.SystemStreams;
import io.joshworks.fstore.serializer.json.JsonSerializer;

//TODO ideally it would update only the diff, currently is just overriding everything into the storage
public class ProjectionUpdated {

    //serializing straight into a StreamMetadata
    public static final String TYPE = EventId.SYSTEM_PREFIX + "PROJECTION_UPDATED";

    public static EventRecord create(Projection metadata) {
        var data = JsonSerializer.toBytes(metadata);
        return EventRecord.create(SystemStreams.PROJECTIONS, TYPE, data);
    }

    public static Projection from(EventRecord record) {
        return JsonSerializer.fromJson(record.data, Projection.class);
    }

}
