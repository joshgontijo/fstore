package io.joshworks.eventry.server.cluster.nodelog;

import io.joshworks.eventry.EventId;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.serializer.json.JsonSerializer;

import java.nio.ByteBuffer;

public class NodeCreatedEvent implements NodeEvent {

    private static final Serializer<NodeCreatedEvent> serializer = JsonSerializer.of(NodeCreatedEvent.class);

    public static final String TYPE = EventId.SYSTEM_PREFIX + "NODE_CREATED";

    public final String nodeId;

    public NodeCreatedEvent(String nodeId) {
        this.nodeId = nodeId;
    }

    public static NodeCreatedEvent from(EventRecord record) {
        return serializer.fromBytes(ByteBuffer.wrap(record.body));
    }

    @Override
    public EventRecord toEvent() {
        ByteBuffer data = serializer.toBytes(this);
        return EventRecord.create(NodeLog.NODES_STREAM, TYPE, data.array());
    }
}
