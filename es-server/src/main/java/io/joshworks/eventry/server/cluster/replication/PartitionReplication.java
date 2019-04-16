package io.joshworks.eventry.server.cluster.replication;

import io.joshworks.eventry.EventLogIterator;
import io.joshworks.eventry.LinkToPolicy;
import io.joshworks.eventry.SystemEventPolicy;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.server.cluster.RemoteIterators;
import io.joshworks.eventry.server.cluster.partition.Partition;
import io.joshworks.eventry.server.cluster.partition.Partitions;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class PartitionReplication {

    private final Partitions partitions;

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(5);
    private final RemoteIterators iterators = new RemoteIterators();

    private final Map<String, String> itMap = new ConcurrentHashMap<>();

    public PartitionReplication(Partitions partitions) {
        this.partitions = partitions;
        this.scheduler.scheduleAtFixedRate(this::fetchFromMaster, 1, 1, TimeUnit.SECONDS);
    }

    private void fetchFromMaster() {
        for (Partition replica : partitions.replicas()) {
            String owner = replica.owner();
            itMap.computeIfAbsent(owner, k -> {
                var it = acquireIterator(replica);
                return iterators.add(10, 100, it);
            });

            String itUuid = itMap.get(replica.owner());
            List<EventRecord> records = iterators.nextBatch(itUuid);


        }

    }

    private EventLogIterator acquireIterator(Partition partition) {
        return partition.store().fromAll(LinkToPolicy.INCLUDE, SystemEventPolicy.INCLUDE);
    }

    private void sendAsync() {

    }

}
