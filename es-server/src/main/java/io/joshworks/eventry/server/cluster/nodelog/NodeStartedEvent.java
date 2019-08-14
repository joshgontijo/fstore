package io.joshworks.eventry.server.cluster.nodelog;

import io.joshworks.fstore.es.shared.EventRecord;
import io.joshworks.fstore.serializer.json.JsonSerializer;

public class NodeStartedEvent implements NodeEvent {

    public static final String TYPE = "NODE_STARTED";

    public final String nodeId;
    public final String host;
    public final int httpPort;
    public final int tcpPort;

    public NodeStartedEvent(String nodeId, String host, int httpPort, int tcpPort) {
        this.nodeId = nodeId;
        this.host = host;
        this.httpPort = httpPort;
        this.tcpPort = tcpPort;
    }

    public static NodeStartedEvent from(EventRecord record) {
        return JsonSerializer.fromJson(record.data, NodeStartedEvent.class);
    }

    @Override
    public EventRecord toEvent() {
        byte[] data = JsonSerializer.toBytes(this);
        return EventRecord.create(NodeLog.NODES_STREAM, TYPE, data);
    }
}
