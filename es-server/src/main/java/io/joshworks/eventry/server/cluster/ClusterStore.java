package io.joshworks.eventry.server.cluster;

import io.joshworks.eventry.EventLogIterator;
import io.joshworks.eventry.EventStore;
import io.joshworks.eventry.IEventStore;
import io.joshworks.eventry.LinkToPolicy;
import io.joshworks.eventry.StreamName;
import io.joshworks.eventry.SystemEventPolicy;
import io.joshworks.eventry.index.IndexEntry;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.projections.Projection;
import io.joshworks.eventry.projections.State;
import io.joshworks.eventry.projections.result.Metrics;
import io.joshworks.eventry.projections.result.TaskStatus;
import io.joshworks.eventry.server.cluster.message.NodeInfoReceived;
import io.joshworks.eventry.server.cluster.message.NodeInfoRequested;
import io.joshworks.eventry.server.cluster.message.NodeJoined;
import io.joshworks.eventry.server.cluster.message.NodeLeft;
import io.joshworks.eventry.stream.StreamInfo;
import io.joshworks.eventry.stream.StreamMetadata;
import io.joshworks.fstore.core.eventbus.EventBus;
import io.joshworks.fstore.core.eventbus.Subscribe;
import io.joshworks.fstore.log.LogIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

public class ClusterStore implements IEventStore {

    private static final Logger logger = LoggerFactory.getLogger(ClusterStore.class);

    private static final int PARTITIONS = 2;

    private final Cluster cluster;
    private final List<Partition> partitions = new ArrayList<>();
    private final File rootDir;
    private final ClusterDescriptor descriptor;
    private final CountDownLatch onlineLatch = new CountDownLatch(1);

    private ClusterStore(File rootDir, Cluster cluster, ClusterDescriptor clusterDescriptor) {
        this.rootDir = rootDir;
        this.descriptor = clusterDescriptor;
        this.cluster = cluster;
    }


    public static ClusterStore connect(File rootDir, String name) {
        try {
            ClusterDescriptor descriptor = ClusterDescriptor.acquire(rootDir);
            EventBus eventBus = new EventBus();
            Cluster cluster = new Cluster(name, descriptor.uuid, eventBus);
            ClusterStore store = new ClusterStore(rootDir, cluster, descriptor);
            eventBus.register(store);
            cluster.join();
            cluster.castAsync(NodeJoined.create(store.descriptor.uuid));



            if (!cluster.otherNodes().isEmpty()) {
                store.onlineLatch.await();
            }

            logger.info("Connected to {}", name);
            return store;

        } catch (Exception e) {
            throw new RuntimeException("Failed to connect to " + name, e);
        }


//        if (store.descriptor.isNew) {
//            logger.info("No local store found, initializing");
//            if (cluster.otherNodes().isEmpty()) {
//                logger.info("No members connected, initializing root store");
//                store.partitions.addAll(initializePartitions(rootDir));
//            } else {
//                logger.info("Members detected, forking streams");
//                cluster.cast(new ForkPartition())
//            }
//        } else {
//            logger.info("Directory already, loading streams");
//            this.partitions.addAll(loadPartitions());
//        }
    }

    @Subscribe
    public void onNodeJoined(NodeJoined nodeJoined) {
        logger.info("Node joined: {}, sending node info", nodeJoined.uuid);

    }

    @Subscribe
    public void onNodeInfoRequested(NodeInfoRequested infoRequested) {
        logger.info("Node info requested from: {}", infoRequested.uuid);
        List<Integer> pids = partitions.stream().map(p -> p.id).collect(Collectors.toList());
        cluster.sendTo(infoRequested.uuid, NodeInfoReceived.create(descriptor.uuid, pids));

    }

    @Subscribe
    public void onNodeLeft(NodeLeft nodeLeft) {
        logger.info("Node left: {}", nodeLeft.uuid);
    }

    @Subscribe
    public void onNodeInfoReceived(NodeInfoReceived nodeInfo) {
        logger.info("Node info received: {}", nodeInfo);
        onlineLatch.countDown();
    }

    private static List<Partition> initializePartitions(File root) {
        List<Partition> newPartitions = new ArrayList<>();
        for (int i = 0; i < PARTITIONS; i++) {
            String pId = "partition-" + i;
            IEventStore store = EventStore.open(new File(root, pId));
            newPartitions.add(new Partition(i, store));
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

    @Override
    public EventRecord append(EventRecord event) {
        Partition partition = select(event.stream);
        return partition.store().append(event);
    }

    @Override
    public void compact() {

    }

    @Override
    public State query(Set<String> streams, State state, String script) {
        return null;
    }

    @Override
    public void close() {
        cluster.castAsync(NodeLeft.create(descriptor.uuid));
    }

    @Override
    public EventRecord linkTo(String stream, EventRecord event) {
        return null;
    }

    @Override
    public EventRecord linkTo(String dstStream, StreamName source, String sourceType) {
        return null;
    }


    @Override
    public EventRecord append(EventRecord event, int expectedVersion) {
        return null;
    }

    @Override
    public EventRecord appendSystemEvent(EventRecord event) {
        return null;
    }

    @Override
    public EventLogIterator fromStream(StreamName stream) {
        return null;
    }

    @Override
    public EventLogIterator fromStreams(String streamPattern) {
        return null;
    }

    @Override
    public EventLogIterator fromStreams(Set<StreamName> streams) {
        return null;
    }

    @Override
    public LogIterator<EventRecord> fromAll(LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy) {
        return null;
    }

    @Override
    public LogIterator<EventRecord> fromAll(LinkToPolicy linkToPolicy, SystemEventPolicy systemEventPolicy, StreamName lastEvent) {
        return null;
    }

    @Override
    public Collection<Projection> projections() {
        return null;
    }

    @Override
    public Projection projection(String name) {
        return null;
    }

    @Override
    public Projection createProjection(String script) {
        return null;
    }

    @Override
    public Projection updateProjection(String name, String script) {
        return null;
    }

    @Override
    public void deleteProjection(String name) {

    }

    @Override
    public void runProjection(String name) {

    }

    @Override
    public void resetProjection(String name) {

    }

    @Override
    public void stopProjectionExecution(String name) {

    }

    @Override
    public void disableProjection(String name) {

    }

    @Override
    public void enableProjection(String name) {

    }

    @Override
    public Map<String, TaskStatus> projectionExecutionStatus(String name) {
        return null;
    }

    @Override
    public Collection<Metrics> projectionExecutionStatuses() {
        return null;
    }

    @Override
    public void createStream(String name) {

    }

    @Override
    public void createStream(String name, int maxCount, long maxAge) {

    }

    @Override
    public StreamMetadata createStream(String stream, int maxCount, long maxAge, Map<String, Integer> permissions, Map<String, String> metadata) {
        return null;
    }

    @Override
    public List<StreamInfo> streamsMetadata() {
        return null;
    }

    @Override
    public Optional<StreamInfo> streamMetadata(String stream) {
        return Optional.empty();
    }

    @Override
    public void truncate(String stream, int version) {

    }

    @Override
    public EventRecord get(StreamName stream) {
        return null;
    }

    @Override
    public EventRecord get(IndexEntry entry) {
        return null;
    }

    @Override
    public EventRecord resolve(EventRecord record) {
        return null;
    }

    @Override
    public int version(String stream) {
        return 0;
    }

}
