package io.joshworks.eventry.server.cluster.message;

import io.joshworks.eventry.StreamName;
import io.joshworks.eventry.data.SystemStreams;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.serializer.json.JsonSerializer;

import java.nio.ByteBuffer;

public class NodeJoined extends ClusterEvent {

    public static final String TYPE = StreamName.SYSTEM_PREFIX + "NODE_JOINED";
    private static final Serializer<NodeJoined> serializer = JsonSerializer.of(NodeJoined.class);

    public NodeJoined(String uuid) {
        super(uuid);
    }

    public static EventRecord create(String uuid) {
        var data = serializer.toBytes(new NodeJoined(uuid));
        return EventRecord.create(SystemStreams.PROJECTIONS, TYPE, data.array());
    }

    public static NodeJoined from(EventRecord record) {
        return serializer.fromBytes(ByteBuffer.wrap(record.body));
    }

}
