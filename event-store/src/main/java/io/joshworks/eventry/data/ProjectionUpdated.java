package io.joshworks.eventry.data;

import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.projections.Projection;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.serializer.json.JsonSerializer;

import java.nio.ByteBuffer;

//TODO ideally it would update only the diff, currently is just overriding everything into the storage
public class ProjectionUpdated {

    //serializing straight into a StreamMetadata
    private static final Serializer<Projection> serializer = JsonSerializer.of(Projection.class);

    public static final String TYPE = Constant.SYSTEM_PREFIX + "PROJECTION_UPDATED";

    public static EventRecord create(Projection metadata) {
        var data = serializer.toBytes(metadata);
        return EventRecord.create(SystemStreams.PROJECTIONS, TYPE, data.array());
    }

    public static Projection from(EventRecord record) {
        return serializer.fromBytes(ByteBuffer.wrap(record.data));
    }

}
