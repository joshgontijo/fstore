package io.joshworks.eventry.server.cluster.nodelog;

import io.joshworks.eventry.StreamName;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.serializer.json.JsonSerializer;

import java.nio.ByteBuffer;

public class PartitionCreatedEvent implements NodeEvent {

    private static final Serializer<PartitionCreatedEvent> serializer = JsonSerializer.of(PartitionCreatedEvent.class);

    public static final String TYPE = StreamName.SYSTEM_PREFIX + "PARTITION_CREATED";

    public final int id;

    public PartitionCreatedEvent(int id) {
        this.id = id;
    }

    public static PartitionCreatedEvent from(EventRecord record) {
        return serializer.fromBytes(ByteBuffer.wrap(record.body));
    }

    @Override
    public EventRecord toEvent() {
        ByteBuffer data = serializer.toBytes(this);
        return EventRecord.create(NodeLog.PARTITIONS_STREAM, TYPE, data.array());
    }
}
