package io.joshworks.eventry.server.cluster.partition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
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

    public Partition select(String stream) {
        int idx = partitioner.select(stream, numPartitions);
        Partition partition = global.get(idx);
        if (partition == null) {
            throw new IllegalStateException("Partition not found for " + stream + ", idx " + idx);
        }
        return partition;
    }

    public void add(Partition partition) {
        if (partition == null) {
            throw new IllegalArgumentException("Partition must not be empty");
        }
        if (global.putIfAbsent(partition.id, partition) != null) {
            throw new IllegalStateException("Partition already exists");
        }
        if (partition.ownedBy(nodeId)) {
            partitions.put(partition.id, partition);
        }
        if (partition.replicatedBy(nodeId)) {
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

    @Override
    public void close() {
        global.values().forEach(Partition::close);
        global.clear();
    }

    public Collection<Partition> all() {
        return global.values();
    }
}
