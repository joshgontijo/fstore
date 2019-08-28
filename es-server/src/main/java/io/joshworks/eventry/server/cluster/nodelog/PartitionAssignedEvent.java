package io.joshworks.eventry.server.cluster.nodelog;

import io.joshworks.fstore.es.shared.EventRecord;
import io.joshworks.fstore.serializer.json.JsonSerializer;

public class PartitionAssignedEvent implements NodeEvent {

    public static final String TYPE = "PARTITION_ASSIGNED";

    public final String nodeId;

    public PartitionAssignedEvent(String nodeId) {
        this.nodeId = nodeId;
    }

    public static PartitionAssignedEvent from(EventRecord record) {
        return JsonSerializer.fromJson(record.data, PartitionAssignedEvent.class);
    }

    @Override
    public EventRecord toEvent() {
        byte[] data = JsonSerializer.toBytes(this);
        return EventRecord.create(NodeLog.NODES_STREAM, TYPE, data);
    }
}
