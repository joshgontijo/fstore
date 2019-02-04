package io.joshworks.eventry.server.cluster.message;

import io.joshworks.eventry.StreamName;
import io.joshworks.eventry.data.SystemStreams;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.serializer.json.JsonSerializer;

import java.nio.ByteBuffer;

public class PartitionForkRequested extends ClusterEvent {

    public static final String TYPE = StreamName.SYSTEM_PREFIX + "PARTITION_FORK_REQUESTED";
    private static final Serializer<PartitionForkRequested> serializer = JsonSerializer.of(PartitionForkRequested.class);
    public final String from;

    private PartitionForkRequested(String uuid, String from) {
        super(uuid);
        this.from = from;
    }

    public static EventRecord create(String uuid, String from) {
        var data = serializer.toBytes(new PartitionForkRequested( uuid, from));
        return EventRecord.create(SystemStreams.PROJECTIONS, TYPE, data.array());
    }

    public static PartitionForkRequested from(EventRecord record) {
        return serializer.fromBytes(ByteBuffer.wrap(record.body));
    }

}
