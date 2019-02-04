package io.joshworks.eventry.server.cluster.message;

import io.joshworks.eventry.StreamName;
import io.joshworks.eventry.data.SystemStreams;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.serializer.json.JsonSerializer;

import java.nio.ByteBuffer;

public class NodeLeft extends ClusterEvent {

    public static final String TYPE = StreamName.SYSTEM_PREFIX + "NODE_LEFT";
    private static final Serializer<NodeLeft> serializer = JsonSerializer.of(NodeLeft.class);

    private NodeLeft(String uuid) {
        super(uuid);
    }

    public static EventRecord create(String uuid) {
        var data = serializer.toBytes(new NodeLeft(uuid));
        return EventRecord.create(SystemStreams.PROJECTIONS, TYPE, data.array());
    }

    public static NodeLeft from(EventRecord record) {
        return serializer.fromBytes(ByteBuffer.wrap(record.body));
    }

}
