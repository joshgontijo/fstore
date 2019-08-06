package io.joshworks.eventry.server.cluster.nodelog;

import io.joshworks.fstore.es.shared.EventId;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.serializer.json.JsonSerializer;

public class NodeShutdownEvent implements NodeEvent {

    public static final String TYPE = EventId.SYSTEM_PREFIX + "NODE_SHUTDOWN";

    public final String nodeId;

    public NodeShutdownEvent(String nodeId) {
        this.nodeId = nodeId;
    }

    public static NodeShutdownEvent from(EventRecord record) {
        return JsonSerializer.fromBytes(record.body, NodeShutdownEvent.class);
    }

    @Override
    public EventRecord toEvent() {
        byte[] data = JsonSerializer.toBytes(this);
        return EventRecord.create(NodeLog.NODES_STREAM, TYPE, data);
    }
}
