package io.joshworks.eventry.server.cluster.message;

import io.joshworks.eventry.StreamName;
import io.joshworks.eventry.data.SystemStreams;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.serializer.json.JsonSerializer;

import java.nio.ByteBuffer;
import java.util.List;

public class NodeInfoReceived extends ClusterEvent {

    public static final String TYPE = StreamName.SYSTEM_PREFIX + "NODE_INFO_RECEIVED";
    private static final Serializer<NodeInfoReceived> serializer = JsonSerializer.of(NodeInfoReceived.class);

    public final List<Integer> partitions;

    private NodeInfoReceived(String uuid, List<Integer> partitions) {
        super(uuid);
        this.partitions = partitions;
    }

    public static EventRecord create(String uuid, List<Integer> partitions) {
        var data = serializer.toBytes(new NodeInfoReceived(uuid, partitions));
        return EventRecord.create(SystemStreams.PROJECTIONS, TYPE, data.array());
    }

    public static NodeInfoReceived from(EventRecord record) {
        return serializer.fromBytes(ByteBuffer.wrap(record.body));
    }

}
