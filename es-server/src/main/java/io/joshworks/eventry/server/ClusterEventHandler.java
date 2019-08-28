package io.joshworks.eventry.server;

import io.joshworks.eventry.EventStore;
import io.joshworks.eventry.api.EventStoreIterator;
import io.joshworks.eventry.api.IEventStore;
import io.joshworks.eventry.network.Cluster;
import io.joshworks.eventry.network.ClusterNode;
import io.joshworks.eventry.server.cluster.ClusterStoreClient;
import io.joshworks.eventry.server.cluster.Node;
import io.joshworks.eventry.server.cluster.NodeDescriptor;
import io.joshworks.eventry.server.cluster.RemoteIterators;
import io.joshworks.eventry.server.cluster.events.ClusterNodeInfo;
import io.joshworks.eventry.server.cluster.events.NodeInfoRequested;
import io.joshworks.eventry.server.cluster.events.NodeJoined;
import io.joshworks.eventry.server.cluster.events.NodeLeft;
import io.joshworks.eventry.server.cluster.messages.Append;
import io.joshworks.eventry.server.cluster.messages.AppendResult;
import io.joshworks.eventry.server.cluster.messages.CreateStream;
import io.joshworks.eventry.server.cluster.messages.EventBatch;
import io.joshworks.eventry.server.cluster.messages.EventData;
import io.joshworks.eventry.server.cluster.messages.FromAll;
import io.joshworks.eventry.server.cluster.messages.FromStream;
import io.joshworks.eventry.server.cluster.messages.FromStreams;
import io.joshworks.eventry.server.cluster.messages.Get;
import io.joshworks.eventry.server.cluster.messages.IteratorCreated;
import io.joshworks.eventry.server.cluster.messages.IteratorNext;
import io.joshworks.eventry.server.cluster.nodelog.NodeJoinedEvent;
import io.joshworks.eventry.server.cluster.nodelog.NodeLeftEvent;
import io.joshworks.eventry.server.cluster.nodelog.NodeLog;
import io.joshworks.eventry.server.cluster.nodelog.NodeShutdownEvent;
import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.fstore.es.shared.EventId;
import io.joshworks.fstore.es.shared.EventRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ClusterEventHandler implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(ClusterEventHandler.class);

    private final RemoteIterators remoteIterators = new RemoteIterators();

    private final IEventStore localStore;
    private final int httpPort;
    private final int tcpPort;
    private final NodeDescriptor descriptor;
    private final Cluster cluster;
    private final StoreState state;
    private final NodeLog nodeLog;

    public ClusterEventHandler(EventStore localStore, int httpPort, int tcpPort, NodeDescriptor descriptor, Cluster cluster, StoreState state, NodeLog nodeLog) {
        this.localStore = localStore;
        this.httpPort = httpPort;
        this.tcpPort = tcpPort;
        this.descriptor = descriptor;
        this.cluster = cluster;
        this.state = state;
        this.nodeLog = nodeLog;

        registerHandlers(cluster);
    }

    private void registerHandlers(Cluster cluster) {
        cluster.register(NodeInfoRequested.class, this::onNodeInfoRequested);
        cluster.register(NodeJoined.class, this::onNodeJoined);
        cluster.register(NodeLeft.class, this::onNodeLeft);
        cluster.register(ClusterNodeInfo.class, this::onNodeInfoReceived);

        cluster.register(CreateStream.class, this::onStreamCreationRequest);
        cluster.register(IteratorNext.class, this::onIteratorNext);
        cluster.register(FromAll.class, this::fromAll);
        cluster.register(FromStream.class, this::fromStream);
        cluster.register(FromStreams.class, this::fromStreams);
        cluster.register(Append.class, this::append);
        cluster.register(Get.class, this::get);
    }

    private ClusterNodeInfo thisNodeInfo() {
        Set<Integer> streams = state.nodePartitions(descriptor.nodeId());

        Node thisNode = state.getNode(descriptor.nodeId());
        return new ClusterNodeInfo(thisNode.id, thisNode.host, streams);
    }


    private ClusterNodeInfo onNodeJoined(NodeJoined nodeJoined) {
        String nodeId = nodeJoined.nodeId;

        logger.info("Node joined: '{}': {}", nodeId, nodeJoined);
        nodeLog.append(new NodeJoinedEvent(nodeId, nodeJoined.address, nodeJoined.partitions));

        ClusterNode cNode = cluster.node(nodeId);
        IEventStore remoteStore = new ClusterStoreClient(cNode, nodeId, cluster.client());

        Node node = new Node(nodeJoined.nodeId, remoteStore, nodeJoined.address, httpPort, tcpPort);
        state.addNode(node, nodeJoined.partitions);
        return thisNodeInfo();
    }

    private void onNodeLeft(NodeLeft nodeJoined) {
        logger.info("Node left: '{}'", nodeJoined.nodeId);
        nodeLog.append(new NodeLeftEvent(nodeJoined.nodeId));
    }

    private ClusterNodeInfo onNodeInfoRequested(NodeInfoRequested nodeInfoRequested) {
        logger.info("Node info requested from {}", nodeInfoRequested.nodeId);
        Set<Long> streams = localStore.streamsMetadata().stream().map(si -> si.hash).collect(Collectors.toSet());
        return thisNodeInfo();
    }

    private void onNodeInfoReceived(ClusterNodeInfo clusterNodeInfo) {
        logger.info("Node info received from {}: {}", clusterNodeInfo.nodeId, clusterNodeInfo);
    }

    //-------------- STORE RPC ----------------------

    private AppendResult append(Append append) {
        EventRecord created = localStore.append(append.event, append.expectedVersion);
        return new AppendResult(true, created.timestamp, created.version);
    }

    private EventData get(Get get) {
        EventId eventId = EventId.parse(get.streamName);
        EventRecord eventRecord = localStore.get(eventId);
        return new EventData(eventRecord);
    }

    private IteratorCreated fromAll(FromAll fromAll) {
        EventStoreIterator iterator = localStore.fromAll(fromAll.linkToPolicy, fromAll.systemEventPolicy);
        String iteratorId = remoteIterators.add(fromAll.timeout, fromAll.batchSize, iterator);
        return new IteratorCreated(iteratorId);
    }

    private IteratorCreated fromStream(FromStream fromStream) {
        EventId eventId = EventId.parse(fromStream.streamName);
        EventStoreIterator iterator = localStore.fromStream(eventId);
        String iteratorId = remoteIterators.add(fromStream.timeout, fromStream.batchSize, iterator);
        return new IteratorCreated(iteratorId);
    }

    private IteratorCreated fromStreams(FromStreams fromStreams) {
        EventStoreIterator iterator = localStore.fromStreams(fromStreams.eventMap);
        String iteratorId = remoteIterators.add(fromStreams.timeout, fromStreams.batchSize, iterator);
        return new IteratorCreated(iteratorId);
    }

    private EventBatch onIteratorNext(IteratorNext iteratorNext) {
        List<EventRecord> records = remoteIterators.nextBatch(iteratorNext.uuid);
        return new EventBatch(records);
    }

    private StreamMetadata onStreamCreationRequest(CreateStream event) {
        return localStore.createStream(event.stream, event.maxCount, event.maxAge, event.acl, event.metadata);
    }

    @Override
    public void close() {
        remoteIterators.close();
        nodeLog.append(new NodeShutdownEvent(cluster.nodeId()));
    }

}
