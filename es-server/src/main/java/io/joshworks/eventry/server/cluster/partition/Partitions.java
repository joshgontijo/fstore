package io.joshworks.eventry.server.cluster.partition;

import io.joshworks.eventry.EventStore;
import io.joshworks.eventry.IEventStore;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.network.ClusterNode;
import io.joshworks.eventry.server.cluster.RemotePartitionClient;
import io.joshworks.eventry.server.cluster.nodelog.NodeLog;
import io.joshworks.eventry.server.cluster.nodelog.PartitionCreatedEvent;
import io.joshworks.eventry.server.cluster.nodelog.PartitionTransferredEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class Partitions implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(Partitions.class);

    private final int numPartitions;
    private final String nodeId;
    private final Partitioner partitioner = new HashPartitioner();

    //Global state, shared across the cluster
    private final Map<Integer, Partition> global = new ConcurrentHashMap<>();
    //This node partitions
    private final Map<Integer, Partition> partitions = new ConcurrentHashMap<>();
    //This node replicas
    private final Map<Integer, Replica> replicas = new ConcurrentHashMap<>();


    public Partitions(int numPartitions, String nodeId) {
        this.numPartitions = numPartitions;
        this.nodeId = nodeId;

    }

    //Eagerly create partitions
    public void bootstrap(File rootDir, NodeLog nodeLog) {
        for (int id = 0; id < numPartitions; id++) {
            nodeLog.append(new PartitionCreatedEvent(id));
            Partition partition = createLocalPartition(id, rootDir);
            add(partition);
        }
    }

    public Partition select(String stream) {
        int idx = partitioner.select(stream, numPartitions);
        Partition partition = global.get(idx);
        if (partition == null) {
            throw new IllegalStateException("Partition not found for " + stream + ", idx " + idx);
        }
        return partition;
    }

    public void add(Partition partition) {
        if(partition == null) {
            throw new IllegalArgumentException("Partition must not be empty");
        }
        if(global.putIfAbsent(partition.id, partition) != null) {
            throw new IllegalStateException("Partition already exists");
        }
        if(partition.ownedBy(nodeId)) {
            partitions.put(partition.id, partition);
        }
        if(partition.replicatedBy(nodeId)) {
            replicas.put(partition.id, new Replica());
        }
    }

    public Partition get(int id) {
        Partition partition = global.get(id);
        if (partition == null) {
            throw new IllegalArgumentException("No partition for id " + id);
        }
        return partition;
    }

    public Collection<Partition> partitions() {
        return partitions.values();
    }

    public Collection<Replica> replicas() {
        return replicas.values();
    }

    public int numPartitions() {
        return numPartitions;
    }

    private Partition createLocalPartition(int id, File root) {
        String pId = "partition-" + id;
        File partitionRoot = new File(root, pId);
        IEventStore store = EventStore.open(partitionRoot);
//        nodeLog.append(new PartitionCreatedEvent(id));
        return new Partition(id, store);
    }

    private Partition createRemotePartitionClient(String nodeId, int partitionId) {
        ClusterNode node = cluster.node(nodeId);
        IEventStore store = new RemotePartitionClient(node, partitionId, cluster.client());
        return new Partition(partitionId, store);
    }

    public void load(NodeLog nodeLog) {
        logger.info("Loading local partitions");
        Set<Integer> owned = new HashSet<>();

        for (EventRecord record : nodeLog) {
            if (PartitionCreatedEvent.TYPE.equals(record.type)) {
                owned.add(PartitionCreatedEvent.from(record).id);
            }
            if (PartitionTransferredEvent.TYPE.equals(record.type)) {
                PartitionTransferredEvent transferred = PartitionTransferredEvent.from(record);
                if (nodeId.equals(transferred.newNodeId)) {
                    owned.add(transferred.id);
                } else {
                    owned.remove(transferred.id);
                }
            }
        }

        logger.info("Found {} local partitions, opening", owned.size());
        for (Integer partitionId : owned) {
            Partition partition = createLocalPartition(partitionId, rootDir);
            partitions.add(partition);
        }
    }

    @Override
    public void close()  {
        global.values().forEach(Partition::close);
        global.clear();
    }

    public Collection<Partition> all() {
        return global.values();
    }
}
