package io.joshworks.eventry.server.cluster;

import io.joshworks.eventry.EventStore;
import io.joshworks.eventry.IEventStore;
import io.joshworks.eventry.LinkToPolicy;
import io.joshworks.eventry.StreamName;
import io.joshworks.eventry.SystemEventPolicy;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.server.cluster.client.ClusterClient;
import io.joshworks.eventry.server.cluster.client.NodeMessage;
import io.joshworks.eventry.server.cluster.messages.ClusterMessage;
import io.joshworks.eventry.server.cluster.messages.FromAll;
import io.joshworks.eventry.server.cluster.messages.IteratorCreated;
import io.joshworks.eventry.server.cluster.messages.NodeInfo;
import io.joshworks.eventry.server.cluster.messages.NodeInfoRequested;
import io.joshworks.eventry.server.cluster.messages.NodeJoined;
import io.joshworks.eventry.server.cluster.messages.NodeLeft;
import io.joshworks.eventry.server.cluster.messages.PartitionForkCompleted;
import io.joshworks.eventry.server.cluster.messages.PartitionForkRequested;
import io.joshworks.eventry.server.cluster.nodelog.NodeCreatedEvent;
import io.joshworks.eventry.server.cluster.nodelog.NodeJoinedEvent;
import io.joshworks.eventry.server.cluster.nodelog.NodeLeftEvent;
import io.joshworks.eventry.server.cluster.nodelog.NodeLog;
import io.joshworks.eventry.server.cluster.nodelog.NodeStartedEvent;
import io.joshworks.eventry.server.cluster.nodelog.PartitionCreatedEvent;
import io.joshworks.fstore.log.LogIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ClusterStore {

    private static final Logger logger = LoggerFactory.getLogger(ClusterStore.class);

    private static final int PARTITIONS = 4;

    private final Cluster cluster;
    private final Map<Integer, Partition> partitions = new ConcurrentHashMap<>();
    private final File rootDir;
    private final ClusterDescriptor descriptor;

    private final NodeLog nodeLog;
    private final RemoteIterators remoteIterators = new RemoteIterators();


    private ClusterStore(File rootDir, Cluster cluster, ClusterDescriptor clusterDescriptor) {
        this.rootDir = rootDir;
        this.descriptor = clusterDescriptor;
        this.cluster = cluster;
        this.nodeLog = new NodeLog(rootDir);
        this.registerHandlers();
    }

    private void registerHandlers() {
        cluster.register(NodeJoined.CODE, this::onNodeJoined);
        cluster.register(NodeLeft.CODE, this::onNodeLeft);
        cluster.register(NodeInfoRequested.CODE, this::onNodeInfoRequested);
        cluster.register(NodeInfo.CODE, this::onNodeInfoReceived);
        cluster.register(PartitionForkRequested.CODE, this::onPartitionForkRequested);
        cluster.register(PartitionForkCompleted.CODE, this::onPartitionForkCompleted);
        cluster.register(FromAll.CODE, this::fromAllRequested);
    }

    public static ClusterStore connect(File rootDir, String name) {
        try {
            ClusterDescriptor descriptor = ClusterDescriptor.acquire(rootDir);
            Cluster cluster = new Cluster(name, descriptor.nodeId);
            ClusterStore store = new ClusterStore(rootDir, cluster, descriptor);
            store.nodeLog.append(new NodeStartedEvent(descriptor.nodeId));

            ClusterClient clusterClient = cluster.client();
            cluster.join();

            Set<Integer> ownedPartitions = store.nodeLog.ownedPartitions();
            clusterClient.cast(new NodeJoined(store.descriptor.nodeId, ownedPartitions));

            List<NodeMessage> responses = clusterClient.cast(new NodeInfoRequested(descriptor.nodeId));
            if (store.descriptor.isNew) {
                store.nodeLog.append(new NodeCreatedEvent(descriptor.nodeId));
                if (!responses.isEmpty()) {
                    logger.info("Forking partitions");
                    //TODO forking 2 partition from each
                    for (NodeMessage response : responses) {
                        NodeInfo nodeInfo = new NodeInfo(response.buffer);
                        Iterator<Integer> it = nodeInfo.partitions.iterator();
                        for (int i = 0; i < 2; i++) {
                            if (it.hasNext()) {
                                int partitionId = it.next();
                                store.forkPartition(partitionId);
                            }
                        }
                    }
                } else {
                    logger.info("No other nodes found, initializing partitions");
                    store.partitions.putAll(store.initializePartitions(rootDir));
                }
            }

            logger.info("Connected to {}", name);
            return store;

        } catch (Exception e) {
            throw new RuntimeException("Failed to connect to " + name, e);
        }
    }

    private NodeMessage fromAllRequested(NodeMessage message) {
        FromAll fromAll = message.as(FromAll::new);
        LogIterator<EventRecord> iterator = partitions.get(fromAll.partitionId).store().fromAll(fromAll.linkToPolicy, fromAll.systemEventPolicy);
        String iteratorId = remoteIterators.add(-1, -1, iterator);
        return NodeMessage.create(descriptor.nodeId, new IteratorCreated(iteratorId));
    }

    //TODO this is totally wrong, append here will create events from scratch
    private void forkPartition(int partitionId) {
        LogIterator<EventRecord> iterator = partitions.get(partitionId).store().fromAll(LinkToPolicy.INCLUDE, SystemEventPolicy.INCLUDE);
        Partition partition = createPartition(rootDir, partitionId);
        IEventStore store = partition.store();
        iterator.forEachRemaining(store::append);

        cluster.client().cast(new PartitionForkCompleted(partitionId));
    }

    private void onNodeJoined(NodeMessage message) {
        NodeJoined nodeJoined = message.as(NodeJoined::new);
        logger.info("Node joined: '{}': {}", nodeJoined.nodeId, nodeJoined);
        nodeLog.append(new NodeJoinedEvent(nodeJoined.nodeId));
        for (int ownedPartition : nodeJoined.partitions) {
            Partition remotePartition = initRemotePartition(nodeJoined.nodeId, ownedPartition);
            partitions.put(ownedPartition, remotePartition);
        }
    }

    private void onNodeLeft(NodeMessage message) {
        NodeLeft nodeJoined = message.as(NodeLeft::new);
        logger.info("Node left: '{}'", nodeJoined.nodeId);
        nodeLog.append(new NodeLeftEvent(nodeJoined.nodeId));
    }

    private ClusterMessage onNodeInfoRequested(NodeMessage message) {
        NodeInfoRequested nodeInfoRequested = message.as(NodeInfoRequested::new);
        logger.info("Node info requested from {}", nodeInfoRequested.nodeId);
        return new NodeInfo(descriptor.nodeId, partitions.keySet());
    }

    private void onNodeInfoReceived(NodeMessage message) {
        NodeInfo nodeInfo = message.as(NodeInfo::new);
        logger.info("Node info received from {}: {}", nodeInfo.nodeId, nodeInfo);
    }

    private void onPartitionForkRequested(NodeMessage message) {
        PartitionForkRequested fork = message.as(PartitionForkRequested::new);
        logger.info("Partition fork requested: {}", fork);
    }

    private void onPartitionForkCompleted(NodeMessage message) {
        PartitionForkCompleted fork = message.as(PartitionForkCompleted::new);
        logger.info("Partition fork completed: {}", fork.partitionId);
        //TODO ownership of the partition should be transferred
    }

    private Map<Integer, Partition> initializePartitions(File root) {
        Map<Integer, Partition> newPartitions = new HashMap<>();
        for (int i = 0; i < PARTITIONS; i++) {
            newPartitions.put(i, createPartition(root, i));
        }
        return newPartitions;
    }

    private Partition createPartition(File root, int id) {
        String pId = "partition-" + id;
        File partitionRoot = new File(root, pId);
        IEventStore store = EventStore.open(partitionRoot);
        nodeLog.append(new PartitionCreatedEvent(id));
        return new Partition(id, store);
    }

    private Partition loadPartition(File root, int id) {
        throw new UnsupportedOperationException("TODO");
    }

    private Partition initRemotePartition(String nodeId, int partitionId) {
        IEventStore store = new RemoteStoreClient(cluster.client(), nodeId);
        return new Partition(partitionId, store);
    }


    private Partition select(String stream) {
        long hash = StreamName.hash(stream);
        int idx = (int) (Math.abs(hash) % PARTITIONS);
        return partitions.get(idx);
    }


}
