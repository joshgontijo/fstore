package io.joshworks.fstore.server.cluster.nodelog;

import io.joshworks.fstore.es.shared.EventRecord;
import io.joshworks.fstore.serializer.json.JsonSerializer;

public class NodeShutdownEvent implements NodeEvent {

    public static final String TYPE = "NODE_SHUTDOWN";

    public final String nodeId;

    public NodeShutdownEvent(String nodeId) {
        this.nodeId = nodeId;
    }

    public static NodeShutdownEvent from(EventRecord record) {
        return JsonSerializer.fromJson(record.data, NodeShutdownEvent.class);
    }

    @Override
    public EventRecord toEvent() {
        byte[] data = JsonSerializer.toBytes(this);
        return EventRecord.create(NodeLog.NODES_STREAM, TYPE, data);
    }
}
