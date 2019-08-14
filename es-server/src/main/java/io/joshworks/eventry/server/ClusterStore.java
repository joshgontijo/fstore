package io.joshworks.eventry.server;

import io.joshworks.eventry.EventStore;
import io.joshworks.eventry.LinkToPolicy;
import io.joshworks.eventry.SystemEventPolicy;
import io.joshworks.eventry.api.EventStoreIterator;
import io.joshworks.eventry.api.IEventStore;
import io.joshworks.eventry.network.Cluster;
import io.joshworks.eventry.network.MulticastResponse;
import io.joshworks.eventry.server.cluster.NodeDescriptor;
import io.joshworks.eventry.server.cluster.messages.StreamCreated;
import io.joshworks.eventry.server.cluster.node.Node;
import io.joshworks.eventry.server.cluster.nodelog.NodeLog;
import io.joshworks.eventry.stream.StreamInfo;
import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.util.Pair;
import io.joshworks.fstore.es.shared.EventId;
import io.joshworks.fstore.es.shared.EventMap;
import io.joshworks.fstore.es.shared.EventRecord;
import io.joshworks.fstore.es.shared.NodeInfo;
import io.joshworks.fstore.es.shared.Status;
import io.joshworks.fstore.es.shared.streams.StreamHasher;
import io.joshworks.fstore.log.iterators.PeekingIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static io.joshworks.eventry.stream.StreamMetadata.NO_MAX_AGE;
import static io.joshworks.eventry.stream.StreamMetadata.NO_MAX_COUNT;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElseGet;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.reducing;

public class ClusterStore implements IEventStore {

    private static final String LOCAL_STORE_LOCATION = "local";
    private static final Logger logger = LoggerFactory.getLogger(ClusterStore.class);

    private final EventStore localStore;
    private final NodeDescriptor descriptor;
    private final ClusterEventHandler eventHandler;
    private final StoreState state = new StoreState();
    private final NodeLog nodeLog;
    private final Cluster cluster;


    private ClusterStore(File root, Cluster cluster, NodeDescriptor descriptor, int httpPort, int tcpPort) {
        requireNonNull(root, "Root folder must be provided");
        this.descriptor = requireNonNull(descriptor, "Descriptor must be provided");
        this.nodeLog = new NodeLog(root);
        this.cluster = cluster;

        this.localStore = EventStore.open(new File(root, LOCAL_STORE_LOCATION));
        this.eventHandler = new ClusterEventHandler(localStore, httpPort, tcpPort, descriptor, cluster, state, nodeLog);
    }

    public static ClusterStore connect(File rootDir, String name, int httpPort, int tcpPort) {
        NodeDescriptor descriptor = NodeDescriptor.read(rootDir);

        if (descriptor == null) {
            descriptor = NodeDescriptor.write(rootDir, name);
        }
        if (!descriptor.clusterName().equals(name)) {
            throw new IllegalArgumentException("Cannot connect store from cluster " + descriptor.clusterName() + " to another cluster: " + name);
        }

        Cluster cluster = new Cluster(name, descriptor.nodeId());
        ClusterStore clusterStore = new ClusterStore(rootDir, cluster, descriptor, httpPort, tcpPort);

        cluster.join();

        return clusterStore;
    }

    public NodeDescriptor descriptor() {
        return descriptor;
    }

    private List<Node> nodes() {
        return state.nodes();
    }

    public Node thisNode() {
        return state.getNode(descriptor.nodeId());
    }

    public List<NodeInfo> nodesInfo() {
        return state.nodes().stream().map(n -> new NodeInfo(n.id, n.host, n.httpPort, n.tcpPort, n.status)).collect(Collectors.toList());
    }

    public NodeLog nodeLog() {
        return nodeLog;
    }

    private Node select(String stream) {
        return select(StreamHasher.hash(stream));
    }

    private Node select(long streamHash) {
        return state.nodeForStream(streamHash);
    }

    public String nodeId() {
        return descriptor.nodeId();
    }

    private Node nodeForNewStream(String stream) {
        Node node;
        long hash = StreamHasher.hash(stream);
        List<Node> nodes = nodes();
        int nodeIdx = (int) (Math.abs(hash) % nodes.size());
        node = nodes.get(nodeIdx);
        return node;
    }

    public void forEachPartition(Consumer<Node> consumer) {
        for (Node node : nodes()) {
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
        state.updateNode(descriptor.nodeId(), Status.UNAVAILABLE);
        IOUtils.closeQuietly(descriptor);
        IOUtils.closeQuietly(nodeLog);
        IOUtils.closeQuietly(localStore);
    }

    @Override
    public EventRecord linkTo(String stream, EventRecord event) {
        //TODO stream validation might be needed here
        //TODO should only link to a local store


        return select(event.stream).store().linkTo(stream, event);
    }

    @Override
    public EventRecord linkTo(String dstStream, EventId source, String sourceType) {
        return select(source.name()).store().linkTo(dstStream, source, sourceType);
    }

    @Override
    public EventRecord append(EventRecord event) {
        Node owner = select(event.stream);
        if (owner == null) {
            //stream does not exit select from the hash
            owner = nodeForNewStream(event.stream);
        }

        return owner.store().append(event);
    }

    @Override
    public EventRecord append(EventRecord event, int expectedVersion) {
        Node node = select(event.stream);
        EventRecord appended = node.store().append(event, expectedVersion);
        if (appended.version == EventId.START_VERSION) {
            broadcastStreamCreation(event.stream, node);
        }
        return appended;
    }

    @Override
    public EventStoreIterator fromStream(EventId stream) {
        return select(stream.name()).store().fromStream(stream);
    }

    @Override
    public EventStoreIterator fromStreams(EventMap eventMap, Set<String> streamPatterns) {
        List<EventStoreIterator> iterators = nodes().stream()
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
        List<EventStoreIterator> iterators = nodes()
                .stream()
                .map(n -> n.store().fromAll(linkToPolicy, systemEventPolicy))
                .collect(Collectors.toList());

        return new OrderedIterator(iterators);
    }

    @Override
    public EventStoreIterator fromAll(LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy, EventId lastEvent) {
        //TODO handle lastEvent, pass only each individual value to each node
        List<EventStoreIterator> iterators = nodes()
                .stream()
                .map(n -> n.store().fromAll(linkToPolicy, systemEventPolicy, lastEvent))
                .collect(Collectors.toList());

        return new OrderedIterator(iterators);
    }

    public EventStoreIterator fromAll(String nodeId, LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy) {
        return state.getNode(nodeId).store().fromAll(linkToPolicy, systemEventPolicy);
    }

    public EventStoreIterator fromAll(String nodeId, LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy, EventId lastEvent) {
        return state.getNode(nodeId).store().fromAll(linkToPolicy, systemEventPolicy, lastEvent);
    }

    @Override
    public StreamMetadata createStream(String stream) {
        return createStream(stream, NO_MAX_COUNT, NO_MAX_AGE, new HashMap<>(), new HashMap<>());
    }

    @Override
    public StreamMetadata createStream(String stream, int maxCount, int maxAge, Map<String, Integer> acl, Map<String, String> metadata) {
        Node node = select(stream);
        if (node != null) {
            throw new IllegalArgumentException("Stream " + stream + " already exist");
        }

        //hashes the stream and select a node to create this stream
        //this avoids cluster global lock.
        //Can cause problem only if two requests to create the same stream happens at the same time and
        //nodes.size() will return different node idx
        node = nodeForNewStream(stream);

        //TODO add user who created / ACL
        metadata = requireNonNullElseGet(metadata, HashMap::new);
        metadata.put("_node", node.id);


        //TODO add an option to explicitly specify the node where the stream will be created, it does require global lock
        //Another thing cna be done is to return a SEE_OTHER response status and make the client go to another node
        StreamMetadata created = node.store().createStream(stream, maxCount, maxAge, acl, metadata);
        if (node.id.equals(descriptor.nodeId())) {
            broadcastStreamCreation(stream, node);
        }
        return created;
    }

    private void broadcastStreamCreation(String stream, Node node) {
        //this node broadcast the stream creation and add to this stream mapping as well
        state.addStream(StreamHasher.hash(stream), node);
        List<MulticastResponse> responses = cluster.client().cast(new StreamCreated(stream));
        //TODO do nothing with ack ?
    }

    @Override
    public List<StreamInfo> streamsMetadata() {
        return nodes().stream()
                .map(Node::store)
                .flatMap(es -> es.streamsMetadata().stream())
                .collect(Collectors.toList());
    }

    @Override
    public Set<Long> streams() {
        return nodes().stream()
                .map(Node::store)
                .flatMap(es -> es.streams().stream())
                .collect(Collectors.toSet());
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
        private int nodeIdx;

        private PartitionedEventStoreIterator(List<EventStoreIterator> iterators) {
            this.iterators = iterators;
        }

        @Override
        public boolean hasNext() {
            int nodes = iterators.size();
            for (int i = 0; i < nodes; i++, nodeIdx++) {
                if (nodeIdx >= nodes) {
                    nodeIdx = 0;
                }
                if (iterators.get(nodeIdx).hasNext()) {
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
            return iterators.get(nodeIdx).next();
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
