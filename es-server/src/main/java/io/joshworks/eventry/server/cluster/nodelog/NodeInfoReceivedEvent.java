package io.joshworks.eventry.server.cluster.nodelog;

import io.joshworks.fstore.es.shared.EventRecord;
import io.joshworks.fstore.es.shared.Node;
import io.joshworks.fstore.serializer.json.JsonSerializer;

public class NodeInfoReceivedEvent implements NodeEvent {

    public static final String TYPE = "NODE_INFO_RECEIVED";

    public final Node node;

    public NodeInfoReceivedEvent( Node node) {
        this.node = node;
    }

    public static NodeInfoReceivedEvent from(EventRecord record) {
        return JsonSerializer.fromJson(record.data, NodeInfoReceivedEvent.class);
    }

    @Override
    public EventRecord toEvent() {
        byte[] data = JsonSerializer.toBytes(this);
        return EventRecord.create(NodeLog.NODES_STREAM, TYPE, data);
    }
}