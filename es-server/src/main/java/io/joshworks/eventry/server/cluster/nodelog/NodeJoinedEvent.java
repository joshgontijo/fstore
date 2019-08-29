package io.joshworks.eventry.server.cluster.nodelog;

import io.joshworks.fstore.es.shared.EventRecord;
import io.joshworks.fstore.es.shared.Node;
import io.joshworks.fstore.serializer.json.JsonSerializer;

import java.util.Set;

public class NodeJoinedEvent implements NodeEvent {

    public static final String TYPE = "NODE_JOINED";

    public final Node node;

    public NodeJoinedEvent(Node node) {
        this.node = node;
    }

    public static NodeJoinedEvent from(EventRecord record) {
        return JsonSerializer.fromJson(record.data, NodeJoinedEvent.class);
    }

    @Override
    public EventRecord toEvent() {
        byte[] data = JsonSerializer.toBytes(this);
        return EventRecord.create(NodeLog.NODES_STREAM, TYPE, data);
    }
}
