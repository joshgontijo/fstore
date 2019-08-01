package io.joshworks.eventry.partition;

import io.joshworks.eventry.EventId;
import io.joshworks.eventry.EventStore;
import io.joshworks.eventry.utils.StringUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Node implements AutoCloseable {

    private static final String PARTITION_FILE_PREFIX = "partition-";
    private final File root;
    private final String id;

    //This node partitions
    private final Map<Integer, Partition> partitions = new ConcurrentHashMap<>();

    //This node replicas
    private final Map<Integer, Replica> replicas = new ConcurrentHashMap<>();
    //in sync replicas
    private final Map<Integer, Replica> isr = new ConcurrentHashMap<>();

    //All the buckets
    private final Partition[] buckets;

    private final Partitioner partitioner = new HashPartitioner();

    public Node(File root, String id, int numBuckets) {
        this.root = root;
        this.id = id;
        this.buckets = new Partition[numBuckets];
    }

    public Partition select(long streamHash) {
        int idx = partitioner.select(streamHash, buckets.length);
        Partition partition = buckets[idx];
        if (partition == null) {
            throw new IllegalStateException("Partition not found for " + streamHash + ", idx " + idx);
        }
        return partition;
    }

    public Partition select(String stream) {
        if (StringUtils.isBlank(stream)) {
            throw new IllegalArgumentException("Stream cannot be null or empty");
        }
        int idx = partitioner.select(EventId.hash(stream), buckets.length);
        Partition partition = buckets[idx];
        if (partition == null) {
            throw new IllegalStateException("Partition not found for " + stream + ", idx " + idx);
        }
        return partition;
    }

    public synchronized void createPartition(int id, int[] buckets) {
        if (partitions.containsKey(id)) {
            throw new IllegalArgumentException("Partition with id " + id + " already exist");
        }
        for (int bucket : buckets) {
            if (this.buckets[bucket] != null) {
                throw new IllegalArgumentException("Bucket with id " + bucket + " already allocated to " + this.buckets[bucket].id);
            }
        }
        EventStore store = EventStore.open(new File(root, PARTITION_FILE_PREFIX + id));
        Partition partition = new Partition(id, buckets, store);
        partitions.put(partition.id, partition);

        for (int bucket : buckets) {
            this.buckets[bucket] = partition;
        }
    }

    public synchronized void replica(Partition partition) {
        replicas.put(partition.id, new Replica());
    }

    public Partition get(int partitionId) {
        Partition partition = partitions.get(partitionId);
        if (partition == null) {
            throw new IllegalArgumentException("No partition for id " + partitionId);
        }
        return partition;
    }

    public Collection<Partition> partitions() {
        return new ArrayList<>(partitions.values());
    }

    public Collection<Replica> replicas() {
        return new ArrayList<>(replicas.values());
    }

    public int numPartitions() {
        return partitions.size();
    }

    public int buckets() {
        return buckets.length;
    }

    @Override
    public void close() {
        partitions.values().forEach(Partition::close);
        Arrays.fill(buckets, null);
    }
}
