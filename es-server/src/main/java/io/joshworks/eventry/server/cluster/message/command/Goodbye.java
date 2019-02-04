package io.joshworks.eventry.server.cluster.message.command;

import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.server.cluster.message.NodeLeft;
import org.jgroups.Address;

public class Goodbye extends ClusterCommand {

    private final String id;

    public Goodbye(String id) {
        this.id = id;
    }

    @Override
    public EventRecord toEvent(Address address) {
        return NodeLeft.create(address, id);
    }
}
