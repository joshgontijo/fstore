package io.joshworks.eventry.server.cluster.nodelog;

import io.joshworks.eventry.StreamName;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.serializer.json.JsonSerializer;

import java.nio.ByteBuffer;

public class NodeJoinedEvent implements NodeEvent {

    private static final Serializer<NodeJoinedEvent> serializer = JsonSerializer.of(NodeJoinedEvent.class);

    public static final String TYPE = StreamName.SYSTEM_PREFIX + "NODE_JOINED";

    public final String nodeId;

    public NodeJoinedEvent(String nodeId) {
        this.nodeId = nodeId;
    }

    public static NodeJoinedEvent from(EventRecord record) {
        return serializer.fromBytes(ByteBuffer.wrap(record.body));
    }

    @Override
    public EventRecord toEvent() {
        ByteBuffer data = serializer.toBytes(this);
        return EventRecord.create(NodeLog.NODES_STREAM, TYPE, data.array());
    }
}
