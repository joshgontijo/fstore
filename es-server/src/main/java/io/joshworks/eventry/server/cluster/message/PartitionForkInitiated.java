package io.joshworks.eventry.server.cluster.message;

import io.joshworks.eventry.StreamName;
import io.joshworks.eventry.data.SystemStreams;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.serializer.json.JsonSerializer;

import java.nio.ByteBuffer;

public class PartitionForkInitiated extends ClusterEvent {

    public static final String TYPE = StreamName.SYSTEM_PREFIX + "PARTITION_FORK_INITIATED";
    private static final Serializer<PartitionForkInitiated> serializer = JsonSerializer.of(PartitionForkInitiated.class);
    public final int partitionId;

    private PartitionForkInitiated(String uuid, int partitionId) {
        super(uuid);
        this.partitionId = partitionId;
    }

    public static EventRecord create(String uuid, int partitionId) {
        var data = serializer.toBytes(new PartitionForkInitiated(uuid, partitionId));
        return EventRecord.create(SystemStreams.PROJECTIONS, TYPE, data.array());
    }

    public static PartitionForkInitiated from(EventRecord record) {
        return serializer.fromBytes(ByteBuffer.wrap(record.body));
    }

}
