package io.joshworks.eventry.server.cluster.message;

import io.joshworks.eventry.StreamName;
import io.joshworks.eventry.data.SystemStreams;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.serializer.json.JsonSerializer;

import java.nio.ByteBuffer;

public class PartitionForkCompleted extends ClusterEvent {

    public static final String TYPE = StreamName.SYSTEM_PREFIX + "PARTITION_FORK_COMPLETED";
    private static final Serializer<PartitionForkCompleted> serializer = JsonSerializer.of(PartitionForkCompleted.class);
    public final int partitionId;

    private PartitionForkCompleted(String uuid, int partitionId) {
        super(uuid);
        this.partitionId = partitionId;
    }

    public static EventRecord create(String uuid, int partitionId) {
        var data = serializer.toBytes(new PartitionForkCompleted(uuid, partitionId));
        return EventRecord.create(SystemStreams.PROJECTIONS, TYPE, data.array());
    }

    public static PartitionForkCompleted from(EventRecord record) {
        return serializer.fromBytes(ByteBuffer.wrap(record.body));
    }

}
