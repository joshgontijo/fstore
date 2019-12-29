package io.joshworks.fstore.server;

import io.joshworks.fstore.EventStore;
import io.joshworks.fstore.api.EventStoreIterator;
import io.joshworks.fstore.cluster.Cluster;
import io.joshworks.fstore.cluster.NodeInfo;
import io.joshworks.fstore.cluster.MulticastResponse;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.es.shared.EventMap;
import io.joshworks.fstore.es.shared.EventRecord;
import io.joshworks.fstore.es.shared.Status;
import io.joshworks.fstore.es.shared.routing.HashRouter;
import io.joshworks.fstore.es.shared.routing.Router;
import io.joshworks.fstore.es.shared.streams.StreamPattern;
import io.joshworks.fstore.es.shared.utils.StringUtils;
import io.joshworks.fstore.log.iterators.PeekingIterator;
import io.joshworks.fstore.server.cluster.NodeDescriptor;
import io.joshworks.fstore.server.cluster.events.NodeInfoRequest;
import io.joshworks.fstore.server.cluster.events.NodeJoined;
import io.joshworks.fstore.server.cluster.events.NodeLeft;
import io.joshworks.fstore.server.cluster.nodelog.NodeInfoReceivedEvent;
import io.joshworks.fstore.server.cluster.nodelog.NodeJoinedEvent;
import io.joshworks.fstore.server.cluster.nodelog.NodeLeftEvent;
import io.joshworks.fstore.server.cluster.nodelog.NodeLog;
import io.joshworks.fstore.server.cluster.nodelog.NodeShutdownEvent;
import io.joshworks.fstore.server.cluster.nodelog.NodeStartedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;

import static io.joshworks.fstore.es.shared.EventId.START_VERSION;
import static java.util.Objects.requireNonNull;

public class ClusterStore extends EventStore {

    private static final String LOCAL_STORE_LOCATION = "local";

    private static final Logger logger = LoggerFactory.getLogger(ClusterStore.class);

    private final NodeDescriptor descriptor;
    private final ClusterState clusterState;
    private final NodeLog nodeLog;
    private final Cluster cluster;

    private final Router router = new HashRouter();

    private ClusterStore(File root, Cluster cluster, NodeDescriptor descriptor) {
        super(new File(root, LOCAL_STORE_LOCATION));
        this.descriptor = requireNonNull(descriptor, "Descriptor must be provided");
        this.nodeLog = new NodeLog(root);
        this.cluster = cluster;

        this.clusterState = new ClusterState(descriptor.nodeId());

        registerHandlers(cluster);
    }

    public static ClusterStore connect(File rootDir, String clusterName, int httpPort, int tcpPort) {
        NodeDescriptor descriptor = NodeDescriptor.read(rootDir);

        if (descriptor == null) {
            descriptor = NodeDescriptor.write(rootDir, clusterName);
        }
        if (!descriptor.clusterName().equals(clusterName)) {
            throw new IllegalArgumentException("Cannot connect store from cluster " + descriptor.clusterName() + " to another cluster: " + clusterName);
        }

        Cluster cluster = new Cluster(clusterName, descriptor.nodeId());
        ClusterStore store = new ClusterStore(rootDir, cluster, descriptor);

        cluster.join();

        NodeInfo cNode = cluster.node();
        io.joshworks.fstore.es.shared.Node thisNode = new io.joshworks.fstore.es.shared.Node(cNode.id, cNode.hostAddress(), httpPort, tcpPort, Status.ACTIVE);
        store.clusterState.update(thisNode);
        store.nodeLog.append(new NodeStartedEvent(cNode.id, cNode.hostAddress(), httpPort, tcpPort));
        List<MulticastResponse> responses = cluster.client().cast(new NodeJoined(thisNode));
        for (MulticastResponse response : responses) {
            io.joshworks.fstore.server.cluster.events.NodeInfo nodeInfoInfo = response.message();
            logger.info("Received node info: {}", nodeInfoInfo);

            store.clusterState.update(nodeInfoInfo.node);
            store.nodeLog.append(new NodeInfoReceivedEvent(nodeInfoInfo.node));
        }

        return store;
    }

    private void registerHandlers(Cluster cluster) {
        cluster.register(NodeInfoRequest.class, this::onNodeInfoRequested);
        cluster.register(NodeJoined.class, this::onNodeJoined);
        cluster.register(NodeLeft.class, this::onNodeLeft);
    }

    private io.joshworks.fstore.server.cluster.events.NodeInfo onNodeJoined(NodeJoined nodeJoined) {
        logger.info("Node joined: {}", nodeJoined);
        nodeLog.append(new NodeJoinedEvent(nodeJoined.node));
        clusterState.update(nodeJoined.node);
        return new io.joshworks.fstore.server.cluster.events.NodeInfo(clusterState.thisNode());
    }

    private void onNodeLeft(NodeLeft nodeLeft) {
        logger.info("Node left: '{}'", nodeLeft.nodeId);
        nodeLog.append(new NodeLeftEvent(nodeLeft.nodeId));
        clusterState.update(nodeLeft.nodeId, Status.UNAVAILABLE);
    }

    private io.joshworks.fstore.server.cluster.events.NodeInfo onNodeInfoRequested(NodeInfoRequest nodeInfoRequest) {
        logger.info("Node info requested from {}", nodeInfoRequest.nodeId);
        return new io.joshworks.fstore.server.cluster.events.NodeInfo(clusterState.thisNode());
    }

    public io.joshworks.fstore.es.shared.Node thisNode() {
        return clusterState.getNode(descriptor.nodeId());
    }

    public List<io.joshworks.fstore.es.shared.Node> nodesInfo() {
        return clusterState.all().stream().map(n -> new io.joshworks.fstore.es.shared.Node(n.id, n.host, n.httpPort, n.tcpPort, n.status)).collect(Collectors.toList());
    }

    public NodeLog nodeLog() {
        return nodeLog;
    }

    public String nodeId() {
        return descriptor.nodeId();
    }

    @Override
    public EventRecord append(EventRecord event) {
        EventRecord record = super.append(event);
        if (record.version == START_VERSION) {
            //TODO broadcast stream ?
        }
        return record;
    }


    @Override
    public EventRecord append(EventRecord event, int expectedVersion) {
        EventRecord record = super.append(event, expectedVersion);
        if (record.version == START_VERSION) {
            //TODO broadcast stream ?
        }
        return record;
    }

    @Override
    public EventStoreIterator fromStreams(EventMap checkpoint, Set<String> streamPatterns) {
        Map<Boolean, List<String>> items = streamPatterns.stream()
                .map(String::trim)
                .filter(StringUtils::nonBlank)
                .collect(Collectors.partitioningBy(StreamPattern::isWildcard));

        Set<String> wildcard = new HashSet<>(items.get(true));
        Set<String> nonWildcard = new HashSet<>(items.get(false));

        Set<String> thisNodePatterns = new HashSet<>(wildcard);

        for (String pattern : nonWildcard) {
            io.joshworks.fstore.es.shared.Node node = HashRouter.select(clusterState.all(), pattern);
            if (node.equals(thisNode())) {
                thisNodePatterns.add(pattern);
            }
        }

        if (thisNodePatterns.isEmpty()) {
            return null;
        }
        return super.fromStreams(checkpoint, thisNodePatterns);

    }

    @Override
    public void close() {
        //TODO improve
        nodeLog.append(new NodeShutdownEvent(cluster.nodeId()));
        clusterState.update(descriptor.nodeId(), Status.UNAVAILABLE);
        IOUtils.closeQuietly(descriptor);
        IOUtils.closeQuietly(nodeLog);
        super.close();
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
