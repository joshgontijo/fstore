package io.joshworks.eventry.server.cluster;

import io.joshworks.eventry.EventStore;
import io.joshworks.eventry.IEventStore;
import io.joshworks.eventry.StreamName;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.server.cluster.messages.ClusterMessage;
import io.joshworks.eventry.server.cluster.messages.NodeInfo;
import io.joshworks.eventry.server.cluster.messages.NodeInfoRequested;
import io.joshworks.eventry.server.cluster.messages.NodeJoined;
import io.joshworks.eventry.server.cluster.messages.NodeLeft;
import io.joshworks.eventry.server.cluster.messages.PartitionForkCompleted;
import io.joshworks.eventry.server.cluster.messages.PartitionForkInitiated;
import io.joshworks.eventry.server.cluster.messages.PartitionForkRequested;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ClusterStore {

    private static final Logger logger = LoggerFactory.getLogger(ClusterStore.class);

    private static final int PARTITIONS = 10;

    private final Cluster cluster;
    private final List<Partition> partitions = new ArrayList<>();
    private final File rootDir;
    private final ClusterDescriptor descriptor;

    private ClusterStore(File rootDir, Cluster cluster, ClusterDescriptor clusterDescriptor) {
        this.rootDir = rootDir;
        this.descriptor = clusterDescriptor;
        this.cluster = cluster;
        this.registerHandlers();
    }

    private void registerHandlers() {
        cluster.register(NodeJoined.CODE, this::onNodeJoined);
        cluster.register(NodeLeft.CODE, this::onNodeLeft);
        cluster.register(NodeInfoRequested.CODE, this::onNodeInfoRequested);
        cluster.register(NodeInfo.CODE, this::onNodeInfoReceived);
        cluster.register(PartitionForkRequested.TYPE, this::onPartitionForkRequested);
        cluster.register(PartitionForkInitiated.TYPE, this::onPartitionForkInitiated);
        cluster.register(PartitionForkCompleted.TYPE, this::onPartitionForkCompleted);
    }

    public static ClusterStore connect(File rootDir, String name) {
        try {
            ClusterDescriptor descriptor = ClusterDescriptor.acquire(rootDir);
            Cluster cluster = new Cluster(name, descriptor.uuid);
            ClusterStore store = new ClusterStore(rootDir, cluster, descriptor);

            cluster.join();
            cluster.cast(NodeJoined.create(store.descriptor.uuid));

            recordChannel.join(store);

            List<ClusterMessage> responses = cluster.cast(NodeInfoRequested.create(descriptor.uuid));
            if (store.descriptor.isNew) {
                if (!responses.isEmpty()) {
                    logger.info("Forking partitions");
                    //TODO forking 2 partition from each
                    for (ClusterMessage response : responses) {
                        EventRecord message = response.message();
                        NodeInfo nodeInfo = NodeInfo.from(message);
                        for (int i = 0; i < 2; i++) {
                            int partitionId = nodeInfo.partitions.get(i);
                            cluster.sendAsync(response.sender(), PartitionForkRequested.create(descriptor.uuid, partitionId));
                        }
                    }
                } else {
                    logger.info("No other nodes found, initializing partitions");
                    store.partitions.addAll(initializePartitions(rootDir));
                }
            }

            logger.info("Connected to {}", name);
            return store;

        } catch (Exception e) {
            throw new RuntimeException("Failed to connect to " + name, e);
        }
    }

    private void onNodeJoined(ByteBuffer message) {
        NodeJoined nodeJoined = new NodeJoined(message);
        logger.info("Node joined: '{}'", nodeJoined.nodeId);
    }

    private void onNodeLeft(ByteBuffer message) {
        NodeLeft nodeJoined = new NodeLeft(message);
        logger.info("Node left: '{}'", nodeJoined.nodeId);
    }

    private ClusterMessage onNodeInfoRequested(ByteBuffer message) {
        NodeInfoRequested nodeInfoRequested = new NodeInfoRequested(message);
        logger.info("Node info requested from {}", nodeInfoRequested.nodeId);
        List<Integer> pids = partitions.stream().map(p -> p.id).collect(Collectors.toList());
        return new NodeInfo(descriptor.uuid, pids);
    }

    private void onNodeInfoReceived(ByteBuffer message) {
        NodeInfo nodeInfo = new NodeInfo(message);
        logger.info("Node info received from {}", nodeInfo.nodeId);
    }

    private ClusterMessage onPartitionForkRequested(ClusterMessage message) {
        PartitionForkRequested fork = PartitionForkRequested.from(message.message());
        logger.info("Partition fork requested");
        Partition partition = partitions.stream().filter(p -> p.id == fork.partitionId).findAny().orElseThrow(() -> new IllegalArgumentException("No partition found for id " + fork.uuid));
        partition.close(); //TODO disable partition ?

        cluster.transferPartition(message.sender(), partition.root())
                .thenRun(() -> cluster.send(message.sender(), PartitionForkCompleted.create(descriptor.uuid, fork.partitionId)));
    }

    private ClusterMessage onPartitionForkInitiated(ClusterMessage message) {
        PartitionForkInitiated fork = PartitionForkInitiated.from(message.message());
        logger.info("Node info initiated: {}", fork.partitionId);
    }

    private ClusterMessage onPartitionForkCompleted(ClusterMessage message) {
        PartitionForkInitiated fork = PartitionForkInitiated.from(message.message());
        logger.info("Partition fork completed: {}", fork.partitionId);
    }

    private static List<Partition> initializePartitions(File root) {
        List<Partition> newPartitions = new ArrayList<>();
        for (int i = 0; i < PARTITIONS; i++) {
            String pId = "partition-" + i;
            File partitionRoot = new File(root, pId);
            IEventStore store = EventStore.open(partitionRoot);
            newPartitions.add(new Partition(i, partitionRoot, store));
        }
        return newPartitions;
    }

    private List<Partition> loadPartitions() {
        throw new UnsupportedOperationException("TODO");
    }

    private Partition select(String stream) {
        long hash = StreamName.hash(stream);
        int idx = (int) (Math.abs(hash) % PARTITIONS);
        return partitions.get(idx);
    }




}
