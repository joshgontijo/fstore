package io.joshworks.eventry.server.cluster.message;

import io.joshworks.eventry.StreamName;
import io.joshworks.eventry.data.SystemStreams;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.serializer.json.JsonSerializer;
import org.jgroups.Address;

import java.nio.ByteBuffer;

public class PartitionTransferred extends ClusterEvent {

    public static final String TYPE = StreamName.SYSTEM_PREFIX + "PARTITION_TRANSFERRED";
    private static final Serializer<PartitionTransferred> serializer = JsonSerializer.of(PartitionTransferred.class);
    public final String from;

    private PartitionTransferred(String uuid, String from) {
        super(uuid);
        this.from = from;
    }

    public static EventRecord create(Address address, String uuid, String from) {
        var data = serializer.toBytes(new PartitionTransferred(uuid, from));
        return EventRecord.create(SystemStreams.PROJECTIONS, TYPE, data.array());
    }

    public static PartitionTransferred from(EventRecord record) {
        return serializer.fromBytes(ByteBuffer.wrap(record.body));
    }

}
