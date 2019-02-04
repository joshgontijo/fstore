package io.joshworks.eventry.server.cluster.message;

import io.joshworks.eventry.StreamName;
import io.joshworks.eventry.data.SystemStreams;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.serializer.json.JsonSerializer;

import java.nio.ByteBuffer;

public class PartitionCreated extends ClusterEvent {

    public static final String TYPE = StreamName.SYSTEM_PREFIX + "PARTITION_CREATED";
    private static final Serializer<PartitionCreated> serializer = JsonSerializer.of(PartitionCreated.class);
    public final String from;

    private PartitionCreated(String uuid, String from) {
        super(uuid);
        this.from = from;
    }

    public static EventRecord create(String uuid, String from) {
        var data = serializer.toBytes(new PartitionCreated(uuid, from));
        return EventRecord.create(SystemStreams.PROJECTIONS, TYPE, data.array());
    }

    public static PartitionCreated from(EventRecord record) {
        return serializer.fromBytes(ByteBuffer.wrap(record.body));
    }

}
