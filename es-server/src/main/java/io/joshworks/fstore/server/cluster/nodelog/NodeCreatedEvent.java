package io.joshworks.fstore.server.cluster.nodelog;

import io.joshworks.fstore.es.shared.EventRecord;
import io.joshworks.fstore.serializer.json.JsonSerializer;

public class NodeCreatedEvent implements NodeEvent {

    public static final String TYPE = "NODE_CREATED";

    public final String nodeId;

    public NodeCreatedEvent(String nodeId) {
        this.nodeId = nodeId;
    }

    public static NodeCreatedEvent from(EventRecord record) {
        return JsonSerializer.fromJson(record.data, NodeCreatedEvent.class);
    }

    @Override
    public EventRecord toEvent() {
        byte[] data = JsonSerializer.toBytes(this);
        return EventRecord.create(NodeLog.NODES_STREAM, TYPE, data);
    }
}