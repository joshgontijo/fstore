package io.joshworks.eventry.server.cluster.data;

import java.util.List;

public class NodeInfo {

    public final String uuid;
    public final List<Integer> partitions;

    public NodeInfo(String uuid, List<Integer> partitions) {
        this.uuid = uuid;
        this.partitions = partitions;
    }
}
