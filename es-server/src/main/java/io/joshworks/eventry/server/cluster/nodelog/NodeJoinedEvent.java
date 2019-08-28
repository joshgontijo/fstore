package io.joshworks.eventry.server.cluster.nodelog;

import io.joshworks.fstore.es.shared.EventRecord;
import io.joshworks.fstore.serializer.json.JsonSerializer;

import java.util.Set;

public class NodeJoinedEvent implements NodeEvent {

    public static final String TYPE = "NODE_JOINED";

    public final String nodeId;
    public final String address;
    public final Set<Integer> streams;

    public NodeJoinedEvent(String nodeId, String address, Set<Integer> streams) {
        this.nodeId = nodeId;
        this.address = address;
        this.streams = streams;
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
