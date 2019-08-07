package io.joshworks.eventry.server.cluster.node;

import io.joshworks.eventry.EventStore;
import io.joshworks.eventry.LinkToPolicy;
import io.joshworks.eventry.SystemEventPolicy;
import io.joshworks.eventry.api.EventStoreIterator;
import io.joshworks.eventry.api.IEventStore;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.server.cluster.NodeDescriptor;
import io.joshworks.eventry.stream.StreamInfo;
import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.util.Pair;
import io.joshworks.fstore.es.shared.EventId;
import io.joshworks.fstore.es.shared.EventMap;
import io.joshworks.fstore.log.iterators.PeekingIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.reducing;

public class ClusterStore implements IEventStore {

    private static final String LOCAL_NODE = "local";
    private static final Logger logger = LoggerFactory.getLogger(ClusterStore.class);

    //all node partitions
    private final Map<String, Node> nodes = new ConcurrentHashMap<>();
    private final NodeDescriptor descriptor;

    private final Map<Long, Node> streamMapping = new ConcurrentHashMap<>();

    public ClusterStore(File root, NodeDescriptor descriptor) {
        requireNonNull(root, "Root folder must be provided");
        this.descriptor = requireNonNull(descriptor, "Descriptor must be provided");

        EventStore store = EventStore.open(new File(root, LOCAL_NODE));
        nodes.put(descriptor.nodeId(), new Node(descriptor.nodeId(), store));
    }

    public NodeDescriptor descriptor() {
        return descriptor;
    }

    private Node select(String stream) {
        return select(EventId.hash(stream));
    }

    private Node select(long streamHash) {
        Node node = streamMapping.get(streamHash);
        if (node == null) {
            throw new RuntimeException("No node available for " + streamHash);
        }
        return node;
    }

    public String nodeId() {
        return descriptor.nodeId();
    }

    public synchronized Node add(String partitionId, String nodeId, IEventStore store) {
        if (nodes.containsKey(partitionId)) {
            throw new IllegalArgumentException("Partition with id " + partitionId + " already exist");
        }
        Node node = new Node(nodeId, store);
        nodes.put(partitionId, node);
        return node;
    }

    public String partitionOf(String stream) {
        return select(stream).id();
    }

    public void forEachPartition(Consumer<Node> consumer) {
        for (Node node : nodes.values()) {
            consumer.accept(node);
        }
    }

    @Override
    public void compact() {
        forEachPartition(node -> node.store().compact());
    }

    @Override
    public void close() {
        //TODO improve
        Node thisNode = nodes.remove(descriptor.nodeId());
        IOUtils.closeQuietly(descriptor);
        thisNode.store().close();
    }

    @Override
    public EventRecord linkTo(String stream, EventRecord event) {
        //TODO stream validation might be needed here
        return select(event.stream).store().linkTo(stream, event);
    }

    @Override
    public EventRecord linkTo(String dstStream, EventId source, String sourceType) {
        return select(source.name()).store().linkTo(dstStream, source, sourceType);
    }

    @Override
    public EventRecord append(EventRecord event) {
        return select(event.stream).store().append(event);
    }

    @Override
    public EventRecord append(EventRecord event, int expectedVersion) {
        return select(event.stream).store().append(event, expectedVersion);
    }

    @Override
    public EventStoreIterator fromStream(EventId stream) {
        return select(stream.name()).store().fromStream(stream);
    }

    @Override
    public EventStoreIterator fromStreams(EventMap eventMap, Set<String> streamPatterns) {
        List<EventStoreIterator> iterators = nodes.values().stream()
                .map(Node::store)
                .map(s -> s.fromStreams(eventMap, streamPatterns))
                .collect(Collectors.toList());

        return new PartitionedEventStoreIterator(iterators);
    }

    @Override
    public EventStoreIterator fromStreams(EventMap eventMap) {
        //partition -> checkpoint per partition
        Map<Node, EventMap> grouped = eventMap.entrySet()
                .stream()
                .map(s -> Pair.of(s, select(s.getKey())))
                .collect(groupingBy(Pair::right, mapping(kv -> EventMap.of(kv.left.getKey(), kv.left.getValue()), reducing(EventMap.empty(), EventMap::merge))));

        List<EventStoreIterator> iterators = grouped.entrySet()
                .stream()
                .map(kv -> kv.getKey().store().fromStreams(kv.getValue()))
                .collect(Collectors.toList());

        return new PartitionedEventStoreIterator(iterators);
    }

    @Override
    public EventStoreIterator fromAll(LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy) {
        List<EventStoreIterator> iterators = nodes.values()
                .stream()
                .map(n -> n.store().fromAll(linkToPolicy, systemEventPolicy))
                .collect(Collectors.toList());

        return new OrderedIterator(iterators);
    }

    @Override
    public EventStoreIterator fromAll(LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy, EventId lastEvent) {
        //TODO handle lastEvent, pass only each individual value to each node
        List<EventStoreIterator> iterators = nodes.values()
                .stream()
                .map(n -> n.store().fromAll(linkToPolicy, systemEventPolicy, lastEvent))
                .collect(Collectors.toList());

        return new OrderedIterator(iterators);
    }

    public EventStoreIterator fromAll(String partitionId, LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy) {
        return nodes.get(partitionId).store().fromAll(linkToPolicy, systemEventPolicy);
    }

    public EventStoreIterator fromAll(String partitionId, LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy, EventId lastEvent) {
        return nodes.get(partitionId).store().fromAll(linkToPolicy, systemEventPolicy, lastEvent);
    }

    @Override
    public void createStream(String stream) {
        //TODO add option to specify NodeId
        select(stream).store().createStream(stream);
    }

    @Override
    public void createStream(String stream, int maxCount, long maxAge) {
        //TODO add option to specify NodeId
        select(stream).store().createStream(stream, maxCount, maxAge);
    }

    @Override
    public StreamMetadata createStream(String stream, int maxCount, long maxAge, Map<String, Integer> acl, Map<String, String> metadata) {
        //TODO add option to specify NodeId
        return select(stream).store().createStream(stream, maxCount, maxAge, acl, metadata);
    }

    @Override
    public List<StreamInfo> streamsMetadata() {
        return nodes.values().stream()
                .map(Node::store)
                .flatMap(es -> es.streamsMetadata().stream())
                .collect(Collectors.toList());
    }

    @Override
    public Optional<StreamInfo> streamMetadata(String stream) {
        return select(stream).store().streamMetadata(stream);
    }

    @Override
    public void truncate(String stream, int fromVersion) {
        select(stream).store().truncate(stream, fromVersion);
    }

    @Override
    public EventRecord get(EventId stream) {
        return select(stream.name()).store().get(stream);
    }

    @Override
    public int version(String stream) {
        return select(stream).store().version(stream);
    }

    @Override
    public int count(String stream) {
        return select(stream).store().count(stream);
    }


    //Round robin
    private static final class PartitionedEventStoreIterator implements EventStoreIterator {

        private final List<EventStoreIterator> iterators;
        private int partitionIdx;

        private PartitionedEventStoreIterator(List<EventStoreIterator> iterators) {
            this.iterators = iterators;
        }

        @Override
        public boolean hasNext() {
            for (int i = 0; i < iterators.size(); partitionIdx++) {
                if (partitionIdx >= iterators.size()) {
                    partitionIdx = 0;
                }
                if (iterators.get(partitionIdx).hasNext()) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public EventRecord next() {
            if (!hasNext()) {
                return null;
            }
            return iterators.get(partitionIdx).next();
        }

        @Override
        public void close() {
            for (EventStoreIterator iterator : iterators) {
                IOUtils.closeQuietly(iterator);
            }
        }

        @Override
        public EventMap checkpoint() {
            return iterators.stream()
                    .map(EventStoreIterator::checkpoint)
                    .reduce(EventMap.empty(), EventMap::merge);
        }
    }

    private static class OrderedIterator implements EventStoreIterator {

        private final List<PeekingIterator<EventRecord>> iterators;
        private final Collection<EventStoreIterator> originals;

        OrderedIterator(Collection<EventStoreIterator> iterators) {
            this.originals = iterators;
            this.iterators = iterators.stream().map(PeekingIterator::new).collect(Collectors.toList());
        }

        @Override
        public boolean hasNext() {
            for (PeekingIterator<EventRecord> next : iterators) {
                if (next.hasNext()) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public EventRecord next() {
            if (iterators.isEmpty()) {
                throw new NoSuchElementException();
            }
            Iterator<PeekingIterator<EventRecord>> itit = iterators.iterator();
            PeekingIterator<EventRecord> prev = null;
            while (itit.hasNext()) {
                PeekingIterator<EventRecord> curr = itit.next();
                if (!curr.hasNext()) {
                    itit.remove();
                    continue;
                }
                if (prev == null) {
                    prev = curr;
                    continue;
                }

                int c = Long.compare(prev.peek().timestamp, curr.peek().timestamp);
                prev = c >= 0 ? curr : prev;
            }
            if (prev != null) {
                return prev.next();
            }
            return null;
        }

        @Override
        public void close() {
            iterators.forEach(IOUtils::closeQuietly);
        }

        @Override
        public EventMap checkpoint() {
            return originals.stream()
                    .map(EventStoreIterator::checkpoint)
                    .reduce(EventMap.empty(), EventMap::merge);
        }
    }

}
