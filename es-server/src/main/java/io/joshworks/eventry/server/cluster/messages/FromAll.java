package io.joshworks.eventry.server.cluster.messages;

import io.joshworks.eventry.LinkToPolicy;
import io.joshworks.fstore.es.shared.EventId;
import io.joshworks.eventry.SystemEventPolicy;
import io.joshworks.eventry.network.ClusterMessage;

public class FromAll implements ClusterMessage {

    private static final LinkToPolicy[] ltpItems = LinkToPolicy.values();
    private static final SystemEventPolicy[] sepItems = SystemEventPolicy.values();

    public final int timeout;//seconds
    public final int batchSize;
    public final String partitionId;

    public final String lastEvent;

    public final LinkToPolicy linkToPolicy;
    public final SystemEventPolicy systemEventPolicy;

    public FromAll(int timeout, int batchSize, String partitionId, LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy, EventId lastEvent) {
        this.timeout = timeout;
        this.batchSize = batchSize;
        this.partitionId = partitionId;
        this.linkToPolicy = linkToPolicy;
        this.systemEventPolicy = systemEventPolicy;
        this.lastEvent = lastEvent == null ? null : lastEvent.toString();
    }
}
