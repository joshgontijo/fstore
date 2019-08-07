//package io.joshworks.eventry.server.cluster.node;
//
//import io.joshworks.fstore.es.shared.EventId;
//import io.joshworks.fstore.es.shared.EventMap;
//import io.joshworks.eventry.EventStore;
//import io.joshworks.eventry.LinkToPolicy;
//import io.joshworks.eventry.SystemEventPolicy;
//import io.joshworks.eventry.api.EventStoreIterator;
//import io.joshworks.eventry.api.IEventStore;
//import io.joshworks.eventry.log.EventRecord;
//import io.joshworks.eventry.server.cluster.NodeDescriptor;
//import io.joshworks.eventry.stream.StreamInfo;
//import io.joshworks.eventry.stream.StreamMetadata;
//import io.joshworks.fstore.es.shared.utils.StringUtils;
//import io.joshworks.fstore.core.io.IOUtils;
//import io.joshworks.fstore.core.util.Pair;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.File;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Map;
//import java.util.Optional;
//import java.util.Set;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.function.Consumer;
//import java.util.stream.Collectors;
//import java.util.stream.IntStream;
//
//import static java.util.Objects.requireNonNull;
//import static java.util.stream.Collectors.groupingBy;
//import static java.util.stream.Collectors.mapping;
//import static java.util.stream.Collectors.reducing;
//
//public class PartitionedStore implements IEventStore {
//
//    private static final Logger logger = LoggerFactory.getLogger(PartitionedStore.class);
//
//    public static final String PARTITION_PREFIX = "partition-";
//    private final File root;
//
//    //all node partitions
//    private final Map<String, Node> partitions = new ConcurrentHashMap<>();
//
//    //This node replicas
//    private final Map<String, Replica> replicas = new ConcurrentHashMap<>();
//    //in sync replicas
//    private final Map<String, Replica> isr = new ConcurrentHashMap<>();
//
//    //All the buckets
//    private final Node[] buckets;
//
//    private final Partitioner partitioner = new HashPartitioner();
//    private final NodeDescriptor descriptor;
//
//    public long[] hits;
//
//    public PartitionedStore(File root, NodeDescriptor descriptor) {
//        this.root = requireNonNull(root, "Root folder must be provided");
//        this.descriptor = requireNonNull(descriptor, "Descriptor must be provided");
//        this.buckets = new Node[descriptor.numBuckets()];
//        this.hits = new long[descriptor.numBuckets()];
//
//        for (String partitionId : descriptor.partitions()) {
//            File pFile = new File(root, PARTITION_PREFIX + partitionId);
//            logger.info("Loading partition {}", partitionId);
//            Node node = new Node(partitionId, descriptor.nodeId(), master, EventStore.open(pFile));
//            partitions.put(partitionId, node);
//        }
//    }
//
//    public NodeDescriptor descriptor() {
//        return descriptor;
//    }
//
//    private Node select(long streamHash) {
//        int bucket = partitioner.select(streamHash, buckets.length);
//        Node node = buckets[bucket];
//        if (node == null) {
//            throw new IllegalStateException("Partition not found for " + streamHash + ", idx " + bucket);
//        }
//        hits[bucket] = hits[bucket] + 1;
//        return node;
//    }
//
//    private Node select(String stream) {
//        if (StringUtils.isBlank(stream)) {
//            throw new IllegalArgumentException("Stream cannot be null or empty");
//        }
//        int bucket = partitioner.select(EventId.hash(stream), buckets.length);
//        Node node = buckets[bucket];
//        if (node == null) {
//            throw new IllegalStateException("Partition not found for " + stream + ", bucket " + bucket);
//        }
//        hits[bucket] = hits[bucket] + 1;
//        return node;
//    }
//
//    public String nodeId() {
//        return descriptor.nodeId();
//    }
//
//    public synchronized Node add(String partitionId, String nodeId, IEventStore store) {
//        if (partitions.containsKey(partitionId)) {
//            throw new IllegalArgumentException("Partition with id " + partitionId + " already exist");
//        }
//        Node node = new Node(partitionId, nodeId, master, store);
//        partitions.put(partitionId, node);
//        return node;
//    }
//
//    public synchronized void bootstrapBuckets() {
//        for (Node bucket : buckets) {
//            if (bucket != null) {
//                throw new RuntimeException("Store already contains assigned buckets");
//            }
//        }
//        logger.info("Assigning initial {} buckets to {} partitions", descriptor.numBuckets(), partitions.size());
//
//        List<Integer> buckets = IntStream.range(0, descriptor.numBuckets()).boxed().collect(Collectors.toList());
//        spreadBuckets(buckets, new ArrayList<>(partitions.keySet()));
//    }
//
//    public Set<Integer> spreadBuckets(List<Integer> buckets, List<String> partitions) {
//        if (partitions.isEmpty()) {
//            throw new IllegalArgumentException("Partitions must be provided");
//        }
//        if (buckets.isEmpty()) {
//            throw new IllegalArgumentException("Buckets must be provided");
//        }
//        Set<Integer> assigned = new HashSet<>();
//        int bucketPerPartition = buckets.size() / partitions.size();
//        for (int i = 0; i < partitions.size(); i++) {
//            String pid = partitions.get(i);
//            int startBucket = i * bucketPerPartition;
//            int endBucket = (i * bucketPerPartition) + bucketPerPartition;
//            endBucket = i == partitions.size() - 1 ? buckets.size() : endBucket;
//            Set<Integer> actualAssigned = assignBuckets(pid, new HashSet<>(buckets.subList(startBucket, endBucket)));
//            assigned.addAll(actualAssigned);
//        }
//        return assigned;
//    }
//
//    public Set<Integer> assignBuckets(String partitionId, Set<Integer> buckets) {
//        Node node = partitions.get(partitionId);
//        if (node == null) {
//            throw new IllegalArgumentException("Partition with id " + partitionId + " not found");
//        }
//        Set<Integer> actuallyAssigned = new HashSet<>();
//        for (int bucket : buckets) {
//            if (this.buckets[bucket] != node) {
//                this.buckets[bucket] = node;
//                actuallyAssigned.add(bucket);
//            }
//        }
//        return actuallyAssigned;
//    }
//
//    public Set<String> partitionIds() {
//        return new HashSet<>(partitions.keySet());
//    }
//
//    public Set<String> nodePartitions(String nodeId) {
//        return partitions.values().stream()
//                .filter(p -> p.nodeId().equals(nodeId))
//                .map(Node::id)
//                .collect(Collectors.toSet());
//    }
//
//    public Set<Integer> nodeBuckets(String nodeId) {
//        return IntStream.range(0, buckets.length)
//                .mapToObj(i -> Pair.of(i, buckets[i]))
//                .filter(p -> p.right != null)
//                .filter(p -> p.right.nodeId().equals(nodeId))
//                .map(Pair::left)
//                .collect(Collectors.toSet());
//    }
//
//    public String partitionOf(String stream) {
//        return select(stream).id();
//    }
//
//    public int numPartitions() {
//        return partitions.size();
//    }
//
//    public Map<String, Set<Integer>> partitionBuckets() {
//        Map<String, Set<Integer>> thisNodeBuckets = new HashMap<>();
//        for (int i = 0; i < buckets.length; i++) {
//            Node node = buckets[i];
//            if (node != null && descriptor.nodeId().equals(node.nodeId())) {
//                thisNodeBuckets.putIfAbsent(node.id(), new HashSet<>());
//                thisNodeBuckets.get(node.id()).add(i);
//            }
//        }
//        return thisNodeBuckets;
//    }
//
//    public void forEachPartition(Consumer<Node> consumer) {
//        for (Node node : partitions.values()) {
//            consumer.accept(node);
//        }
//    }
//
//    @Override
//    public void compact() {
//        forEachPartition(node -> node.store().compact());
//    }
//
//    @Override
//    public void close() {
//        IOUtils.closeQuietly(descriptor);
//        forEachPartition(Node::close);
//    }
//
//    @Override
//    public EventRecord linkTo(String stream, EventRecord event) {
//        //TODO stream validation might be needed here
//        return select(event.stream).store().linkTo(stream, event);
//    }
//
//    @Override
//    public EventRecord linkTo(String dstStream, EventId source, String sourceType) {
//        return select(source.name()).store().linkTo(dstStream, source, sourceType);
//    }
//
//    @Override
//    public EventRecord append(EventRecord event) {
//        return select(event.stream).store().append(event);
//    }
//
//    @Override
//    public EventRecord append(EventRecord event, int expectedVersion) {
//        return select(event.stream).store().append(event, expectedVersion);
//    }
//
//    @Override
//    public EventStoreIterator fromStream(EventId stream) {
//        return select(stream.name()).store().fromStream(stream);
//    }
//
//    @Override
//    public EventStoreIterator fromStreams(EventMap eventMap, Set<String> streamPatterns) {
//        List<EventStoreIterator> iterators = partitions.values().stream()
//                .map(Node::store)
//                .map(s -> s.fromStreams(eventMap, streamPatterns))
//                .collect(Collectors.toList());
//
//        return new PartitionedEventStoreIterator(iterators);
//    }
//
//    @Override
//    public EventStoreIterator fromStreams(EventMap eventMap) {
//        //partition -> checkpoint per partition
//        Map<Node, EventMap> grouped = eventMap.entrySet()
//                .stream()
//                .map(s -> Pair.of(s, select(s.getKey())))
//                .collect(groupingBy(Pair::right, mapping(kv -> EventMap.of(kv.left.getKey(), kv.left.getValue()), reducing(EventMap.empty(), EventMap::merge))));
//
//        List<EventStoreIterator> iterators = grouped.entrySet()
//                .stream()
//                .map(kv -> kv.getKey().store().fromStreams(kv.getValue()))
//                .collect(Collectors.toList());
//
//        return new PartitionedEventStoreIterator(iterators);
//    }
//
//    @Override
//    public EventStoreIterator fromAll(LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy) {
//        throw new UnsupportedOperationException("From all is not supported in a partitioned store");
//    }
//
//    @Override
//    public EventStoreIterator fromAll(LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy, EventId lastEvent) {
//        throw new UnsupportedOperationException("From all is not supported in a partitioned store");
//    }
//
//    public EventStoreIterator fromAll(int partitionId, LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy) {
//        return partitions.get(partitionId).store().fromAll(linkToPolicy, systemEventPolicy);
//    }
//
//    public EventStoreIterator fromAll(int partitionId, LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy, EventId lastEvent) {
//        return partitions.get(partitionId).store().fromAll(linkToPolicy, systemEventPolicy, lastEvent);
//    }
//
//    @Override
//    public void createStream(String stream) {
//        select(stream).store().createStream(stream);
//    }
//
//    @Override
//    public void createStream(String stream, int maxCount, long maxAge) {
//        select(stream).store().createStream(stream, maxCount, maxAge);
//    }
//
//    @Override
//    public StreamMetadata createStream(String stream, int maxCount, long maxAge, Map<String, Integer> acl, Map<String, String> metadata) {
//        return select(stream).store().createStream(stream, maxCount, maxAge, acl, metadata);
//    }
//
//    @Override
//    public List<StreamInfo> streamsMetadata() {
//        return partitions.values().stream()
//                .map(Node::store)
//                .flatMap(es -> es.streamsMetadata().stream())
//                .collect(Collectors.toList());
//    }
//
//    @Override
//    public Optional<StreamInfo> streamMetadata(String stream) {
//        return select(stream).store().streamMetadata(stream);
//    }
//
//    @Override
//    public void truncate(String stream, int fromVersion) {
//        select(stream).store().truncate(stream, fromVersion);
//    }
//
//    @Override
//    public EventRecord get(EventId stream) {
//        return select(stream.name()).store().get(stream);
//    }
//
//    @Override
//    public int version(String stream) {
//        return select(stream).store().version(stream);
//    }
//
//    @Override
//    public int count(String stream) {
//        return select(stream).store().count(stream);
//    }
//
//
//    //Round robin
//    private static final class PartitionedEventStoreIterator implements EventStoreIterator {
//
//        private final List<EventStoreIterator> iterators;
//        private int partitionIdx;
//
//        private PartitionedEventStoreIterator(List<EventStoreIterator> iterators) {
//            this.iterators = iterators;
//        }
//
//        @Override
//        public boolean hasNext() {
//            for (int i = 0; i < iterators.size(); partitionIdx++) {
//                if (partitionIdx >= iterators.size()) {
//                    partitionIdx = 0;
//                }
//                if (iterators.get(partitionIdx).hasNext()) {
//                    return true;
//                }
//            }
//            return false;
//        }
//
//        @Override
//        public EventRecord next() {
//            if (!hasNext()) {
//                return null;
//            }
//            return iterators.get(partitionIdx).next();
//        }
//
//        @Override
//        public void close() {
//            for (EventStoreIterator iterator : iterators) {
//                IOUtils.closeQuietly(iterator);
//            }
//        }
//
//        @Override
//        public EventMap checkpoint() {
//            return iterators.stream()
//                    .map(EventStoreIterator::checkpoint)
//                    .reduce(EventMap.empty(), EventMap::merge);
//        }
//    }
//}
