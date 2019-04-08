package io.joshworks.eventry.server.cluster;

import io.joshworks.eventry.EventLogIterator;
import io.joshworks.eventry.EventStore;
import io.joshworks.eventry.IEventStore;
import io.joshworks.eventry.LinkToPolicy;
import io.joshworks.eventry.StreamName;
import io.joshworks.eventry.SystemEventPolicy;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.network.Cluster;
import io.joshworks.eventry.network.ClusterMessage;
import io.joshworks.eventry.network.ClusterNode;
import io.joshworks.eventry.network.MulticastResponse;
import io.joshworks.eventry.server.cluster.messages.Append;
import io.joshworks.eventry.server.cluster.messages.AppendResult;
import io.joshworks.eventry.server.cluster.messages.EventBatch;
import io.joshworks.eventry.server.cluster.messages.FromAll;
import io.joshworks.eventry.server.cluster.messages.FromStream;
import io.joshworks.eventry.server.cluster.messages.IteratorCreated;
import io.joshworks.eventry.server.cluster.messages.IteratorNext;
import io.joshworks.eventry.server.cluster.messages.NodeInfo;
import io.joshworks.eventry.server.cluster.messages.NodeInfoRequested;
import io.joshworks.eventry.server.cluster.messages.NodeJoined;
import io.joshworks.eventry.server.cluster.messages.NodeLeft;
import io.joshworks.eventry.server.cluster.messages.PartitionAssigned;
import io.joshworks.eventry.server.cluster.messages.PartitionForkCompleted;
import io.joshworks.eventry.server.cluster.messages.PartitionForkRequested;
import io.joshworks.eventry.server.cluster.nodelog.NodeJoinedEvent;
import io.joshworks.eventry.server.cluster.nodelog.NodeLeftEvent;
import io.joshworks.eventry.server.cluster.nodelog.NodeLog;
import io.joshworks.eventry.server.cluster.nodelog.NodeStartedEvent;
import io.joshworks.eventry.server.cluster.nodelog.PartitionCreatedEvent;
import io.joshworks.eventry.server.cluster.partition.Partition;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.log.LogIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ClusterManager implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(ClusterManager.class);

    private final int numPartitions;

    private final Cluster cluster;
    private final Map<Integer, Partition> partitions = new ConcurrentHashMap<>();
    private final File rootDir;
    private final ClusterDescriptor descriptor;

    private final NodeLog nodeLog;
    private final RemoteIterators remoteIterators = new RemoteIterators();

    private ClusterManager(File rootDir, Cluster cluster, ClusterDescriptor clusterDescriptor, int numPartitions) {
        this.rootDir = rootDir;
        this.descriptor = clusterDescriptor;
        this.cluster = cluster;
        this.numPartitions = numPartitions;
        this.nodeLog = new NodeLog(rootDir);
        this.registerHandlers();

//        cluster.interceptor(new LoggingInterceptor());

    }

    private void registerHandlers() {
        cluster.register(NodeJoined.class, this::onNodeJoined);
        cluster.register(NodeLeft.class, this::onNodeLeft);
        cluster.register(NodeInfoRequested.class, this::onNodeInfoRequested);
        cluster.register(NodeInfo.class, this::onNodeInfoReceived);
        cluster.register(PartitionForkRequested.class, this::onPartitionForkRequested);
        cluster.register(PartitionForkCompleted.class, this::onPartitionForkCompleted);
        cluster.register(FromAll.class, this::fromAllRequested);
        cluster.register(FromStream.class, this::fromStreamRequested);
        cluster.register(IteratorNext.class, this::onIteratorNext);
        cluster.register(PartitionAssigned.class, this::onPartitionAssigned);
        cluster.register(Append.class, this::appendFromRemote);
    }

    public static ClusterManager connect(File rootDir, String name, int numPartitions) {
        ClusterDescriptor descriptor = ClusterDescriptor.acquire(rootDir);
        Cluster cluster = new Cluster(name, descriptor.nodeId);
        ClusterManager store = new ClusterManager(rootDir, cluster, descriptor, numPartitions);
        store.nodeLog.append(new NodeStartedEvent(descriptor.nodeId));
        try {

            cluster.join();

            Set<Integer> ownedPartitions = store.nodeLog.ownedPartitions();
            List<MulticastResponse> responses = cluster.client().cast(new NodeJoined(store.descriptor.nodeId, ownedPartitions));
            for (MulticastResponse response : responses) {
                NodeInfo nodeInfo = response.message();
                for (Integer partitionId : nodeInfo.partitions) {
                    store.partitions.put(partitionId, store.createRemotePartition(nodeInfo.nodeId, partitionId));
                }
            }

            logger.info("Connected to {}", name);
            return store;

        } catch (Exception e) {
            IOUtils.closeQuietly(store);
            IOUtils.closeQuietly(descriptor);
            cluster.leave();
            throw new RuntimeException("Failed to connect to " + name, e);
        }
    }

//    private void initPartitions() {
//        //TODO Lock
//        List<MulticastResponse> responses = cluster.client().cast(new NodeInfoRequested(descriptor.nodeId));
//        if (descriptor.isNew) {
//            nodeLog.append(new NodeCreatedEvent(descriptor.nodeId));
//            if (!responses.isEmpty()) {
//                logger.info("Forking partitions");
//                //TODO forking 2 partition from each
//                for (MulticastResponse response : responses) {
//                    NodeInfo nodeInfo = response.message();
//                    Iterator<Integer> it = nodeInfo.partitions.iterator();
//                    for (int i = 0; i < 2; i++) {
//                        if (it.hasNext()) {
//                            int partitionId = it.next();
//                            forkPartition(partitionId);
//                        }
//                    }
//                }
//            } else {
//                logger.info("No other nodes found, initializing partitions");
//                partitions.putAll(initializePartitions());
//            }
//        }
//    }

    public void assignPartition(int partitionId) {
        Partition localPartition = createLocalPartition(partitionId, rootDir);
        partitions.put(localPartition.id, localPartition);
        nodeLog.append(new PartitionCreatedEvent(partitionId));
        cluster.client().cast(new PartitionAssigned(partitionId, descriptor.nodeId));
    }

    private ClusterMessage fromAllRequested(FromAll fromAll) {
        LogIterator<EventRecord> iterator = partitions.get(fromAll.partitionId).store().fromAll(fromAll.linkToPolicy, fromAll.systemEventPolicy);
        String iteratorId = remoteIterators.add(fromAll.timeout, fromAll.batchSize, iterator);
        return new IteratorCreated(iteratorId);
    }

    private ClusterMessage fromStreamRequested(FromStream fromStream) {
        StreamName streamName = StreamName.parse(fromStream.streamName);
        LogIterator<EventRecord> iterator = select(streamName.name()).store().fromStream(streamName);
        String iteratorId = remoteIterators.add(fromStream.timeout, fromStream.batchSize, iterator);
        return new IteratorCreated(iteratorId);
    }

    private ClusterMessage onIteratorNext(IteratorNext iteratorNext) {
        List<EventRecord> records = remoteIterators.nextBatch(iteratorNext.uuid);
        return new EventBatch(records);
    }

    //TODO this is totally wrong, append here will create events from scratch
    private void forkPartition(int partitionId) {
        LogIterator<EventRecord> iterator = partitions.get(partitionId).store().fromAll(LinkToPolicy.INCLUDE, SystemEventPolicy.INCLUDE);
        Partition partition = createEmptyPartition(partitionId);
        IEventStore store = partition.store();
        iterator.forEachRemaining(store::append);

        cluster.client().cast(new PartitionForkCompleted(partitionId));
    }

    private NodeInfo onNodeJoined(NodeJoined nodeJoined) {
        logger.info("Node joined: '{}': {}", nodeJoined.nodeId, nodeJoined);
        nodeLog.append(new NodeJoinedEvent(nodeJoined.nodeId));
        for (int ownedPartition : nodeJoined.partitions) {
            Partition remotePartition = createRemotePartition(nodeJoined.nodeId, ownedPartition);
            partitions.put(ownedPartition, remotePartition);
        }

        return new NodeInfo(descriptor.nodeId, nodeLog.ownedPartitions());
    }

    private void onPartitionAssigned(PartitionAssigned partitionAssigned) {
        logger.info("Partition assigned node: {}, partition: {}", partitionAssigned.nodeId, partitionAssigned.id);
        Partition remotePartition = createRemotePartition(partitionAssigned.nodeId, partitionAssigned.id);
        partitions.put(partitionAssigned.id, remotePartition);
    }

    private void onNodeLeft(NodeLeft nodeJoined) {
        logger.info("Node left: '{}'", nodeJoined.nodeId);
        nodeLog.append(new NodeLeftEvent(nodeJoined.nodeId));
    }

    private ClusterMessage onNodeInfoRequested(NodeInfoRequested nodeInfoRequested) {
        logger.info("Node info requested from {}", nodeInfoRequested.nodeId);
        return new NodeInfo(descriptor.nodeId, partitions.keySet());
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

    private Map<Integer, Partition> initializePartitions() {
        Map<Integer, Partition> newPartitions = new HashMap<>();
        for (int i = 0; i < numPartitions; i++) {
            newPartitions.put(i, createEmptyPartition(i));
        }
        return newPartitions;
    }

    private Partition createLocalPartition(int id, File root) {
        String pId = "partition-" + id;
        File partitionRoot = new File(root, pId);
        IEventStore store = EventStore.open(partitionRoot);
//        nodeLog.append(new PartitionCreatedEvent(id));
        return new Partition(id, store);
    }

    private Partition createEmptyPartition(int id) {
        return new Partition(id, null);
    }


    private Partition loadPartition(File root, int id) {
        throw new UnsupportedOperationException("TODO");
    }

    private Partition createRemotePartition(String nodeId, int partitionId) {
        ClusterNode node = cluster.node(nodeId);
        IEventStore store = new RemotePartitionClient(node, partitionId, cluster.client());
        return new Partition(partitionId, store);
    }

    private Partition select(String stream) {
        long hash = StreamName.hash(stream);
        int idx = (int) (Math.abs(hash) % numPartitions);
        Partition partition = partitions.get(idx);
        if (partition == null) {
            throw new IllegalStateException("Partition not found for " + stream + ", idx " + idx);
        }
        return partition;
    }

    //-------------- PUBLIC API ----------------------

    public void append(EventRecord event) {
        Partition partition = select(event.stream);
        partition.store().append(event);
    }

    public EventRecord get(StreamName streamName) {
        Partition partition = select(streamName.name());
        return partition.store().get(streamName);
    }

    public EventLogIterator fromStream(StreamName streamName) {
        Partition partition = select(streamName.name());
        return partition.store().fromStream(streamName);
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

    private AppendResult appendFromRemote(Append append) {
        Partition partition = select(append.event.stream);
        if (!(partition.store() instanceof EventStore)) {
            //TODO
            throw new IllegalArgumentException("Not a local partition");
        }
        EventRecord created = partition.store().append(append.event, append.expectedVersion);
        return new AppendResult(true);
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(descriptor);
        cluster.leave();
        IOUtils.closeQuietly(remoteIterators);
        partitions.values().forEach(IOUtils::closeQuietly);
        IOUtils.closeQuietly(nodeLog);
    }
}
