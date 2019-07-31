package io.joshworks.eventry.server.cluster.nodelog;

import io.joshworks.eventry.EventId;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.serializer.json.JsonSerializer;

import java.nio.ByteBuffer;

public class NodeStartedEvent implements NodeEvent {

    private static final Serializer<NodeStartedEvent> serializer = JsonSerializer.of(NodeStartedEvent.class);

    public static final String TYPE = EventId.SYSTEM_PREFIX + "NODE_STARTED";

    public final String nodeId;

    public NodeStartedEvent(String nodeId) {
        this.nodeId = nodeId;
    }

    public static NodeStartedEvent from(EventRecord record) {
        return serializer.fromBytes(ByteBuffer.wrap(record.body));
    }

    @Override
    public EventRecord toEvent() {
        ByteBuffer data = serializer.toBytes(this);
        return EventRecord.create(NodeLog.NODES_STREAM, TYPE, data.array());
    }
}
