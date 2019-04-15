package io.joshworks.eventry.server.cluster;

import io.joshworks.eventry.EventStore;
import io.joshworks.eventry.IEventStore;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.network.Cluster;
import io.joshworks.eventry.network.ClusterMessage;
import io.joshworks.eventry.network.ClusterNode;
import io.joshworks.eventry.network.MulticastResponse;
import io.joshworks.eventry.server.cluster.messages.Append;
import io.joshworks.eventry.server.cluster.messages.FromAll;
import io.joshworks.eventry.server.cluster.messages.FromStream;
import io.joshworks.eventry.server.cluster.messages.FromStreams;
import io.joshworks.eventry.server.cluster.messages.Get;
import io.joshworks.eventry.server.cluster.messages.IteratorNext;
import io.joshworks.eventry.server.cluster.events.NodeInfo;
import io.joshworks.eventry.server.cluster.events.NodeInfoRequested;
import io.joshworks.eventry.server.cluster.events.NodeJoined;
import io.joshworks.eventry.server.cluster.events.NodeLeft;
import io.joshworks.eventry.server.cluster.events.PartitionAssigned;
import io.joshworks.eventry.server.cluster.events.PartitionForkCompleted;
import io.joshworks.eventry.server.cluster.events.PartitionForkRequested;
import io.joshworks.eventry.server.cluster.nodelog.NodeJoinedEvent;
import io.joshworks.eventry.server.cluster.nodelog.NodeLeftEvent;
import io.joshworks.eventry.server.cluster.nodelog.NodeLog;
import io.joshworks.eventry.server.cluster.nodelog.NodeStartedEvent;
import io.joshworks.eventry.server.cluster.nodelog.PartitionCreatedEvent;
import io.joshworks.eventry.server.cluster.nodelog.PartitionTransferredEvent;
import io.joshworks.eventry.server.cluster.partition.Partition;
import io.joshworks.fstore.core.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ClusterManager implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(ClusterManager.class);

    private final Cluster cluster;
    private final Partitions partitions;
    private final File rootDir;
    private final ClusterDescriptor descriptor;

    private final NodeLog nodeLog;

    private final ClusterStore store;
    private final StoreReceiver storeReceiver;

    private ClusterManager(File rootDir, Cluster cluster, Partitions partitions, ClusterDescriptor clusterDescriptor, ClusterStore store) {
        this.rootDir = rootDir;
        this.partitions = partitions;
        this.descriptor = clusterDescriptor;
        this.cluster = cluster;
        this.store = store;
        this.nodeLog = new NodeLog(rootDir);
        this.storeReceiver = new StoreReceiver(store);
        this.registerHandlers();
    }

    private void registerHandlers() {
        cluster.register(NodeJoined.class, this::onNodeJoined);
        cluster.register(NodeLeft.class, this::onNodeLeft);
        cluster.register(NodeInfoRequested.class, this::onNodeInfoRequested);
        cluster.register(NodeInfo.class, this::onNodeInfoReceived);
        cluster.register(PartitionForkRequested.class, this::onPartitionForkRequested);
        cluster.register(PartitionForkCompleted.class, this::onPartitionForkCompleted);
        cluster.register(PartitionAssigned.class, this::onPartitionAssigned);

        cluster.register(IteratorNext.class, storeReceiver::onIteratorNext);
        cluster.register(FromAll.class, storeReceiver::fromAll);
        cluster.register(FromStream.class, storeReceiver::fromStream);
        cluster.register(FromStreams.class, storeReceiver::fromStreams);
        cluster.register(Append.class, storeReceiver::append);
        cluster.register(Get.class, storeReceiver::get);
    }

    private void requestNodeInfo() {
        Set<Integer> ownedPartitions = nodeLog.ownedPartitions();
        if (descriptor.isNew && ownedPartitions.isEmpty()) {
            logger.info("No nodes connected and no partitions, initializing...");
            initPartitions();
            return;
        }
        loadPartitions();

        logger.info("Fetching node info");
        List<MulticastResponse> responses = cluster.client().cast(new NodeJoined(descriptor.nodeId, ownedPartitions));
        for (MulticastResponse response : responses) {
            NodeInfo nodeInfo = response.message();
            logger.info("Got {}", nodeInfo);
            for (Integer partitionId : nodeInfo.partitions) {
                Partition partition = createRemotePartition(nodeInfo.nodeId, partitionId);
                partitions.add(partition);
            }
        }
    }

    //Eagerly create partitions
    private void initPartitions() {
        for (int id = 0; id < partitions.numPartitions(); id++) {
            nodeLog.append(new PartitionCreatedEvent(id));
            Partition partition = createLocalPartition(id, rootDir);
            partitions.add(partition);
        }
    }

    public IEventStore store() {
        return store;
    }

    public static ClusterManager connect(File rootDir, String name, int numPartitions) {
        ClusterDescriptor descriptor = ClusterDescriptor.acquire(rootDir);
        Cluster cluster = new Cluster(name, descriptor.nodeId);

        Partitions partitions = new Partitions(numPartitions);
        ClusterStore store = new ClusterStore(partitions);
        ClusterManager manager = new ClusterManager(rootDir, cluster, partitions, descriptor, store);
        try {
            manager.nodeLog.append(new NodeStartedEvent(descriptor.nodeId));
            cluster.join();
            logger.info("Connected to {}", name);
            manager.requestNodeInfo();
            return manager;
        } catch (Exception e) {
            IOUtils.closeQuietly(manager);
            IOUtils.closeQuietly(descriptor);
            cluster.leave();
            throw new RuntimeException("Failed to connect to " + name, e);
        }
    }

    private void loadPartitions() {
        logger.info("Loading local partitions");
        Set<Integer> owned = new HashSet<>();

        for (EventRecord record : nodeLog) {
            if (PartitionCreatedEvent.TYPE.equals(record.type)) {
                owned.add(PartitionCreatedEvent.from(record).id);
            }
            if (PartitionTransferredEvent.TYPE.equals(record.type)) {
                PartitionTransferredEvent transferred = PartitionTransferredEvent.from(record);
                if (descriptor.nodeId.equals(transferred.newNodeId)) {
                    owned.add(transferred.id);
                } else {
                    owned.remove(transferred.id);
                }
            }
        }

        logger.info("Found {} local partitions, opening", owned.size());
        for (Integer partitionId : owned) {
            Partition partition = createLocalPartition(partitionId, rootDir);
            partitions.add(partition);
        }
    }

    public void assignPartition(int partitionId) {
        Partition localPartition = createLocalPartition(partitionId, rootDir);
        partitions.add(localPartition);
        nodeLog.append(new PartitionCreatedEvent(partitionId));
        cluster.client().cast(new PartitionAssigned(partitionId, descriptor.nodeId));
    }

    private NodeInfo onNodeJoined(NodeJoined nodeJoined) {
        logger.info("Node joined: '{}': {}", nodeJoined.nodeId, nodeJoined);
        nodeLog.append(new NodeJoinedEvent(nodeJoined.nodeId));
        for (int ownedPartition : nodeJoined.partitions) {
            Partition remotePartition = createRemotePartition(nodeJoined.nodeId, ownedPartition);
            partitions.add(remotePartition);
        }

        return new NodeInfo(descriptor.nodeId, nodeLog.ownedPartitions());
    }

    private void onPartitionAssigned(PartitionAssigned partitionAssigned) {
        logger.info("Partition assigned node: {}, partition: {}", partitionAssigned.nodeId, partitionAssigned.id);
        Partition remotePartition = createRemotePartition(partitionAssigned.nodeId, partitionAssigned.id);
        partitions.add(remotePartition);
    }

    private void onNodeLeft(NodeLeft nodeJoined) {
        logger.info("Node left: '{}'", nodeJoined.nodeId);
        nodeLog.append(new NodeLeftEvent(nodeJoined.nodeId));
    }

    private ClusterMessage onNodeInfoRequested(NodeInfoRequested nodeInfoRequested) {
        logger.info("Node info requested from {}", nodeInfoRequested.nodeId);
        return new NodeInfo(descriptor.nodeId, partitions.owned().stream().map(p -> p.id).collect(Collectors.toSet()));
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

    private Partition createLocalPartition(int id, File root) {
        String pId = "partition-" + id;
        File partitionRoot = new File(root, pId);
        IEventStore store = EventStore.open(partitionRoot);
//        nodeLog.append(new PartitionCreatedEvent(id));
        return new Partition(id, store);
    }

    private Partition createRemotePartition(String nodeId, int partitionId) {
        ClusterNode node = cluster.node(nodeId);
        IEventStore store = new RemotePartitionClient(node, partitionId, cluster.client());
        return new Partition(partitionId, store);
    }

    //-------------- CLUSTER CMDS ----------------------

    //careful here to not send wrong stream to wrong partition
    void appendToPartition(EventRecord event, int partitionId) {
        Partition partition = partitions.get(partitionId);
        partition.store().append(event);
    }

    void readFromPartition(EventRecord event, int partitionId) {
        Partition partition = partitions.get(partitionId);
        partition.store().append(event);
    }

    @Override
    public void close() {
        nodeLog.append(new NodeStartedEvent(descriptor.nodeId));
        IOUtils.closeQuietly(descriptor);
        cluster.leave();
        IOUtils.closeQuietly(storeReceiver);
        IOUtils.closeQuietly(store);
        IOUtils.closeQuietly(nodeLog);
    }
}
