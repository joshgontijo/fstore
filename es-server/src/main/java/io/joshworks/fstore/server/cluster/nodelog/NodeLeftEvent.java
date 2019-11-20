package io.joshworks.fstore.server.cluster.nodelog;

import io.joshworks.fstore.es.shared.EventRecord;
import io.joshworks.fstore.serializer.json.JsonSerializer;

public class NodeLeftEvent implements NodeEvent {

    public static final String TYPE = "NODE_LEFT";

    public final String nodeId;

    public NodeLeftEvent(String nodeId) {
        this.nodeId = nodeId;
    }

    public static NodeLeftEvent from(EventRecord record) {
        return JsonSerializer.fromJson(record.data, NodeLeftEvent.class);
    }

    @Override
    public EventRecord toEvent() {
        byte[] data = JsonSerializer.toBytes(this);
        return EventRecord.create(NodeLog.NODES_STREAM, TYPE, data);
    }
}
