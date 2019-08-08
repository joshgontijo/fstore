package io.joshworks.eventry.server.cluster.nodelog;

import io.joshworks.fstore.es.shared.EventRecord;
import io.joshworks.fstore.serializer.json.JsonSerializer;

import java.util.Set;

public class NodeInfoReceivedEvent implements NodeEvent {

    public static final String TYPE = "NODE_INFO_RECEIVED";

    public final String nodeId;
    public final String address;
    public final Set<Long> streams; //TODO this can be huge

    public NodeInfoReceivedEvent(String nodeId, String address, Set<Long> streams) {
        this.nodeId = nodeId;
        this.address = address;
        this.streams = streams;
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