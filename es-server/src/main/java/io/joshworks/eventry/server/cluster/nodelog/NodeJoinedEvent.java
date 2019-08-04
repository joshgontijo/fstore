package io.joshworks.eventry.server.cluster.nodelog;

import io.joshworks.eventry.EventId;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.serializer.json.JsonSerializer;

public class NodeJoinedEvent implements NodeEvent {

    public static final String TYPE = EventId.SYSTEM_PREFIX + "NODE_JOINED";

    public final String nodeId;

    public NodeJoinedEvent(String nodeId) {
        this.nodeId = nodeId;
    }

    public static NodeJoinedEvent from(EventRecord record) {
        return JsonSerializer.fromBytes(record.body, NodeJoinedEvent.class);
    }

    @Override
    public EventRecord toEvent() {
        byte[] data = JsonSerializer.toBytes(this);
        return EventRecord.create(NodeLog.NODES_STREAM, TYPE, data);
    }
}
