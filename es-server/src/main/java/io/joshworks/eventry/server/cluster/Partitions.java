package io.joshworks.eventry.server.cluster;

import io.joshworks.eventry.StreamName;
import io.joshworks.eventry.server.cluster.partition.Partition;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class Partitions implements AutoCloseable {

    private final Map<Integer, Partition> partitions = new ConcurrentHashMap<>();
    private final int numPartitions;

    public Partitions(int numPartitions) {
        this.numPartitions = numPartitions;
    }

    public Partition select(String stream) {
        long hash = StreamName.hash(stream);
        int idx = (int) (Math.abs(hash) % numPartitions);
        Partition partition = partitions.get(idx);
        if (partition == null) {
            throw new IllegalStateException("Partition not found for " + stream + ", idx " + idx);
        }
        return partition;
    }

    public void add(Partition partition) {
        partitions.put(partition.id, partition);
    }

    public Partition get(int id) {
        Partition partition = partitions.get(id);
        if (partition == null) {
            throw new IllegalArgumentException("No partition for id " + id);
        }
        return partition;
    }

    public Set<Partition> owned() {
        throw new UnsupportedOperationException("TODO");
    }

    public Set<Partition> partitions(String nodeId) {
        return partitions.values().stream().filter(p -> nodeId.equals(p.nodeId())).collect(Collectors.toSet());
    }

    public int numPartitions() {
        return numPartitions;
    }

    @Override
    public void close()  {
        partitions.values().forEach(Partition::close);
        partitions.clear();
    }
}
