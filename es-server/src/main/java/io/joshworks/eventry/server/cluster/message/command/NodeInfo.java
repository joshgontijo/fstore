package io.joshworks.eventry.server.cluster.message.command;

import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.server.cluster.message.NodeInfoReceived;
import org.jgroups.Address;

import java.util.List;

public class NodeInfo extends ClusterCommand {

    public final String nodeId;
    public final List<Integer> partitions;

    public NodeInfo(String nodeId, List<Integer> partitions) {
        this.nodeId = nodeId;
        this.partitions = partitions;
    }

    @Override
    public EventRecord toEvent(Address address) {
        return NodeInfoReceived.create(address, nodeId, partitions);
    }

}
