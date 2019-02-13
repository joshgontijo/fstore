package io.joshworks.eventry.server.cluster.nodelog;

import io.joshworks.eventry.StreamName;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.serializer.json.JsonSerializer;

import java.nio.ByteBuffer;

public class PartitionMovedEvent implements NodeEvent {

    private static final Serializer<PartitionMovedEvent> serializer = JsonSerializer.of(PartitionMovedEvent.class);

    public static final String TYPE = StreamName.SYSTEM_PREFIX + "PARTITION_MOVED";

    public final int id;
    public final String newNodeId;

    public PartitionMovedEvent(int id, String newNodeId) {
        this.id = id;
        this.newNodeId = newNodeId;
    }

    public static PartitionMovedEvent from(EventRecord record) {
        return serializer.fromBytes(ByteBuffer.wrap(record.body));
    }

    @Override
    public EventRecord toEvent() {
        ByteBuffer data = serializer.toBytes(this);
        return EventRecord.create(NodeLog.PARTITIONS_STREAM, TYPE, data.array());
    }
}
