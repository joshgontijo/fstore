package io.joshworks.eventry.server.cluster;

import io.joshworks.eventry.api.IEventStore;
import io.joshworks.eventry.network.Cluster;
import io.joshworks.eventry.network.ClusterMessage;
import io.joshworks.eventry.network.ClusterNode;
import io.joshworks.eventry.network.MulticastResponse;
import io.joshworks.eventry.server.cluster.events.NodeInfo;
import io.joshworks.eventry.server.cluster.events.NodeInfoRequested;
import io.joshworks.eventry.server.cluster.events.NodeJoined;
import io.joshworks.eventry.server.cluster.events.NodeLeft;
import io.joshworks.eventry.server.cluster.events.PartitionCreated;
import io.joshworks.eventry.server.cluster.events.PartitionForkCompleted;
import io.joshworks.eventry.server.cluster.events.PartitionForkRequested;
import io.joshworks.eventry.server.cluster.messages.Ack;
import io.joshworks.eventry.server.cluster.messages.Append;
import io.joshworks.eventry.server.cluster.messages.FromAll;
import io.joshworks.eventry.server.cluster.messages.FromStream;
import io.joshworks.eventry.server.cluster.messages.FromStreams;
import io.joshworks.eventry.server.cluster.messages.Get;
import io.joshworks.eventry.server.cluster.messages.IteratorNext;
import io.joshworks.eventry.server.cluster.events.BucketAssigned;
import io.joshworks.eventry.server.cluster.node.PartitionedStore;
import io.joshworks.eventry.server.cluster.nodelog.NodeJoinedEvent;
import io.joshworks.eventry.server.cluster.nodelog.NodeLeftEvent;
import io.joshworks.eventry.server.cluster.nodelog.NodeLog;
import io.joshworks.eventry.server.cluster.nodelog.NodeShutdownEvent;
import io.joshworks.fstore.core.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.IntStream;

public class ClusterManager implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(ClusterManager.class);

    private static final String PARTITION_LOCK = "PARTITION_LOCK";

    private final Cluster cluster;

    private final NodeLog nodeLog;

    private final PartitionedStore store;
    private final StoreReceiver storeReceiver;

    private ClusterManager(File rootDir, Cluster cluster, PartitionedStore store) {
        this.store = store;
        this.cluster = cluster;
        this.nodeLog = new NodeLog(rootDir);
        this.storeReceiver = new StoreReceiver(store);
        this.registerHandlers();
    }

    private void registerHandlers() {
        cluster.register(NodeInfoRequested.class, this::onNodeInfoRequested);
        cluster.register(NodeJoined.class, this::onNodeJoined);
        cluster.register(NodeLeft.class, this::onNodeLeft);
        cluster.register(NodeInfo.class, this::onNodeInfoReceived);
        cluster.register(BucketAssigned.class, this::onBucketReassigned);
        cluster.register(PartitionForkRequested.class, this::onPartitionForkRequested);
        cluster.register(PartitionForkCompleted.class, this::onPartitionForkCompleted);
        cluster.register(PartitionCreated.class, this::onPartitionCreated);

        cluster.register(IteratorNext.class, storeReceiver::onIteratorNext);
        cluster.register(FromAll.class, storeReceiver::fromAll);
        cluster.register(FromStream.class, storeReceiver::fromStream);
        cluster.register(FromStreams.class, storeReceiver::fromStreams);
        cluster.register(Append.class, storeReceiver::append);
        cluster.register(Get.class, storeReceiver::get);
    }

    private Ack onBucketReassigned(BucketAssigned event) {
        store.assignBuckets(event.partitionId, event.buckets);
        return new Ack();
    }

    private Map<String, NodeInfo> readNodeBuckets(List<MulticastResponse> responses) {
        Map<String, NodeInfo> nodeInfos = new HashMap<>();
        for (MulticastResponse response : responses) {
            NodeInfo nodeInfo = response.message();
            logger.info("Got {}", nodeInfo);
            nodeInfos.put(nodeInfo.nodeId, nodeInfo);
        }
        return nodeInfos;
    }

    public PartitionedStore store() {
        return store;
    }

    public static ClusterManager connect(File rootDir, String name, int numPartitions, int numBuckets) {
        NodeDescriptor descriptor = NodeDescriptor.read(rootDir);
        boolean isNew = descriptor == null;

        if (descriptor == null) {
            String[] partitionIds = IntStream.range(0, numPartitions).boxed().map(i -> UUID.randomUUID().toString().substring(0, 8)).toArray(String[]::new);
            descriptor = NodeDescriptor.write(rootDir, name, numBuckets, partitionIds);
        }
        if (!descriptor.clusterName().equals(name)) {
            throw new IllegalArgumentException("Cannot connect store from cluster " + descriptor.clusterName() + " to another cluster: " + name);
        }

        PartitionedStore store = new PartitionedStore(rootDir, descriptor);
        Cluster cluster = new Cluster(name, descriptor.nodeId());
        cluster.join();

        List<MulticastResponse> responses = cluster.client().cast(new NodeJoined(descriptor.nodeId(), store.partitionIds()));
        logger.info("Received {} node replies", responses.size());
        if (isNew && responses.isEmpty()) {
            store.bootstrapBuckets();
        }

        return new ClusterManager(rootDir, cluster, store);
    }

    private NodeInfo onNodeJoined(NodeJoined nodeJoined) {
        logger.info("Node joined: '{}': {}", nodeJoined.nodeId, nodeJoined);
        nodeLog.append(new NodeJoinedEvent(nodeJoined.nodeId));
        for (String pid : nodeJoined.partitions) {
            ClusterNode node = cluster.node(nodeJoined.nodeId);
            IEventStore remoteStore = new RemotePartitionClient(node, nodeJoined.nodeId, cluster.client());
            store.add(pid, nodeJoined.nodeId, remoteStore);
        }

        return new NodeInfo(store.nodeId(), store.partitionBuckets());
    }

    private void onPartitionCreated(PartitionCreated partitionCreated) {
        logger.info("Partition created on node: {}, partition: {}", partitionCreated.nodeId, partitionCreated.partitionId);

        ClusterNode node = cluster.node(partitionCreated.nodeId);
        IEventStore remoteStore = new RemotePartitionClient(node, partitionCreated.nodeId, cluster.client());
        store.add(partitionCreated.partitionId, partitionCreated.nodeId, remoteStore);
    }

    private void onNodeLeft(NodeLeft nodeJoined) {
        logger.info("Node left: '{}'", nodeJoined.nodeId);
        nodeLog.append(new NodeLeftEvent(nodeJoined.nodeId));
    }

    private ClusterMessage onNodeInfoRequested(NodeInfoRequested nodeInfoRequested) {
        logger.info("Node info requested from {}", nodeInfoRequested.nodeId);
        return new NodeInfo(store.nodeId(), store.partitionBuckets());
    }

    private void onNodeInfoReceived(NodeInfo nodeInfo) {
        logger.info("Node info received from {}: {}", nodeInfo.nodeId, nodeInfo);
    }

    private void onPartitionForkRequested(PartitionForkRequested fork) {
        logger.info("Partition fork requested: {}", fork);
    }

    private void onPartitionForkCompleted(PartitionForkCompleted fork) {
        logger.info("Partition fork completed: {}", fork.partitionId);
        //TODO ownership of the partition should be transferred
    }


    //-------------- CLUSTER CMDS ----------------------

    public synchronized void reassignBuckets(String targetPartition, Set<Integer> buckets) {
        Set<Integer> actuallyAssigned = store.assignBuckets(targetPartition, buckets);
        cluster.client().cast(new BucketAssigned(targetPartition, actuallyAssigned));
    }

    public synchronized Set<Integer> nodeBuckets(String nodeId) {
        return store.nodeBuckets(nodeId);
    }

    public synchronized Set<Integer> partitionBuckets(String partitionId) {
        return store.partitionBuckets().get(partitionId);
    }

    public synchronized Map<String, Set<Integer>> buckets() {
        return store.partitionBuckets();
    }

    @Override
    public void close() {
        nodeLog.append(new NodeShutdownEvent(store.nodeId()));
        cluster.leave();
        IOUtils.closeQuietly(store);
        IOUtils.closeQuietly(storeReceiver);
        IOUtils.closeQuietly(store);
        IOUtils.closeQuietly(nodeLog);
    }
}
