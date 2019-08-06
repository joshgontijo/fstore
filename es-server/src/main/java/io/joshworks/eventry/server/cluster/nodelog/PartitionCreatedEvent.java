package io.joshworks.eventry.server.cluster.nodelog;

import io.joshworks.fstore.es.shared.EventId;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.serializer.json.JsonSerializer;

public class PartitionCreatedEvent implements NodeEvent {

    public static final String TYPE = EventId.SYSTEM_PREFIX + "PARTITION_CREATED";

    public final int id;

    public PartitionCreatedEvent(int id) {
        this.id = id;
    }

    public static PartitionCreatedEvent from(EventRecord record) {
        return JsonSerializer.fromBytes(record.body, PartitionCreatedEvent.class);
    }

    @Override
    public EventRecord toEvent() {
        byte[] data = JsonSerializer.toBytes(this);
        return EventRecord.create(NodeLog.PARTITIONS_STREAM, TYPE, data);
    }
}
