package io.joshworks.eventry.server.cluster.nodelog;

import io.joshworks.eventry.EventId;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.serializer.json.JsonSerializer;

import java.nio.ByteBuffer;

public class PartitionTransferredEvent implements NodeEvent {

    private static final Serializer<PartitionTransferredEvent> serializer = JsonSerializer.of(PartitionTransferredEvent.class);

    public static final String TYPE = EventId.SYSTEM_PREFIX + "PARTITION_MOVED";

    public final int id;
    public final String newNodeId;

    public PartitionTransferredEvent(int id, String newNodeId) {
        this.id = id;
        this.newNodeId = newNodeId;
    }

    public static PartitionTransferredEvent from(EventRecord record) {
        return serializer.fromBytes(ByteBuffer.wrap(record.body));
    }

    @Override
    public EventRecord toEvent() {
        ByteBuffer data = serializer.toBytes(this);
        return EventRecord.create(NodeLog.PARTITIONS_STREAM, TYPE, data.array());
    }
}
