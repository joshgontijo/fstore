package io.joshworks.eventry.server.cluster.message.command;

import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.server.cluster.message.PartitionForked;
import org.jgroups.Address;

public class ForkPartition extends ClusterCommand {

    public final String id;
    public final String from;

    public ForkPartition(String id, String from) {
        this.id = id;
        this.from = from;
    }

    @Override
    public EventRecord toEvent(Address address) {
        return PartitionForked.create(address, id, from);
    }
}
