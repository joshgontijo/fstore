package io.joshworks.eventry.server.cluster.message;

import io.joshworks.eventry.StreamName;
import io.joshworks.eventry.data.SystemStreams;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.serializer.json.JsonSerializer;

import java.nio.ByteBuffer;
import java.util.List;

public class NodeInfo extends ClusterEvent {

    public static final String TYPE = StreamName.SYSTEM_PREFIX + "NODE_INFO";
    private static final Serializer<NodeInfo> serializer = JsonSerializer.of(NodeInfo.class);

    public final List<Integer> partitions;

    private NodeInfo(String uuid, List<Integer> partitions) {
        super(uuid);
        this.partitions = partitions;
    }

    public static EventRecord create(String uuid, List<Integer> partitions) {
        var data = serializer.toBytes(new NodeInfo(uuid, partitions));
        return EventRecord.create(SystemStreams.PROJECTIONS, TYPE, data.array());
    }

    public static NodeInfo from(EventRecord record) {
        return serializer.fromBytes(ByteBuffer.wrap(record.body));
    }
}
