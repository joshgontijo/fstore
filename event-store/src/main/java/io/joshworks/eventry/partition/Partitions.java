package io.joshworks.eventry.partition;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Partitions implements AutoCloseable {

    private final int numPartitions;
    private final String nodeId;
    private final Partitioner partitioner = new HashPartitioner();

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
        Partition partition = partitions.get(idx);
        if (partition == null) {
            throw new IllegalStateException("Partition not found for " + stream + ", idx " + idx);
        }
        return partition;
    }

    public void add(Partition partition) {
        if(partition == null) {
            throw new IllegalArgumentException("Partition must not be empty");
        }
        if(partition.ownedBy(nodeId)) {
            partitions.put(partition.id, partition);
        }
        if(partition.replicatedBy(nodeId)) {
            replicas.put(partition.id, new Replica());
        }
    }

    public Partition get(int id) {
        Partition partition = partitions.get(id);
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
    public void close()  {
        partitions.values().forEach(Partition::close);
        partitions.clear();
    }

    public Collection<Partition> all() {
        return partitions.values();
    }
}
