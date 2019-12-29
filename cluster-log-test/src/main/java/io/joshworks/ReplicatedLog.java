package io.joshworks;

import io.joshworks.fstore.cluster.Cluster;
import io.joshworks.fstore.cluster.NodeInfo;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.appender.LogAppender;
import io.joshworks.fstore.log.appender.naming.NamingStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.String.format;

public class ReplicatedLog implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(ReplicatedLog.class);
    private final static String CLUSTER_NAME = "MY-CLUSTER";

    private final AtomicBoolean closed = new AtomicBoolean();
    private final LogAppender<Record> log;
    private final Cluster cluster;
    private final String nodeId;
    private final int clusterSize; //TODO update dynamically
    private final AtomicBoolean leader = new AtomicBoolean();
    private final AtomicLong lastSequence = new AtomicLong(-1);

    private final Coordinator coordinator = new Coordinator();

    //Only used by leader node
    private final Map<String, AtomicLong> commitIndexes = new ConcurrentHashMap<>();
    private final int replPort;

    private ReplicationServer replicationServer;
    private ReplicationClient replicationClient;


    public ReplicatedLog(File root, String nodeId, int clusterSize) {
        this.nodeId = nodeId;
        this.clusterSize = clusterSize;
        this.log = LogAppender.builder(new File(root, nodeId), new EntrySerializer())
                .namingStrategy(new SequenceNaming())
                .open();

        lastSequence.set(findLastSequence());
        commitIndexes.put(nodeId, new AtomicLong());
        commitIndexes.get(nodeId).set(lastSequence.get()); //FIXME: cannot set last commit index before checking with other nodes

        this.replPort = randomPort();

        this.cluster = new Cluster(CLUSTER_NAME, nodeId);
        this.cluster.registerRpcProxy(ClusterRpc.class);
        this.cluster.onNodeUpdated(this::onNodeConnected);
        this.cluster.join();

    }

    private long findLastSequence() {
        try (LogIterator<Record> iterator = log.iterator(Direction.BACKWARD)) {
            return iterator.hasNext() ? iterator.next().sequence : 0;
        }
    }

    private synchronized void onNodeConnected(NodeInfo nodeInfo) {
        if (!cluster.isCoordinator()) {
            return;
        }

        if (!hasQuorum()) {
            logger.info("Not enough nodes for a quorum, no action...");
            return;
        }

        coordinator.electLeader(cluster, lastSequence.get());



    }

    public long lastSequence() {
        return lastSequence.get();
    }

    public boolean leader() {
        return leader.get();
    }

    //basic implementation: return only when all nodes are in sync
    public long append(ByteBuffer data, WriteLevel writeLevel) {
        if (!hasQuorum()) {
            throw new RuntimeException("Write rejected: No quorum");
        }
        if (!leader()) {
            //TODO do something smarter ?
            throw new RuntimeException("Not a leader");
        }

        System.out.println("[" + nodeId + "] Append");

        long idx = log.entries();
        long recordPos = log.append(new Record(idx, data));
        lastSequence.set(idx);

        replicationServer.waitForReplication(writeLevel, idx);
        commitIndexes.get(nodeId).set(idx);

        return recordPos;
    }

    private boolean hasQuorum() {
        return cluster.numNodes() >= (clusterSize / 2) + 1;
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        cluster.close();
        log.close();
    }

    private static int randomPort() {
        try {
            ServerSocket serverSocket = new ServerSocket(0);
            int port = serverSocket.getLocalPort();
            serverSocket.close();
            return port;
        } catch (IOException e) {
            throw new RuntimeException("Failed to acquire port");
        }
    }

    private class ClusterRpcHandler implements ClusterRpc {

        @Override
        public void becomeLeader() {
            if (!leader.compareAndSet(false, true)) {
                throw new RuntimeException("Already leader");
            }
            ReplicatedLog.this.replicationClient.close();
            //starts replication server
            InetSocketAddress replPort = new InetSocketAddress("localhost", ReplicatedLog.this.replPort);
            ReplicatedLog.this.replicationServer = new ReplicationServer(replPort, log);
        }

        @Override
        public void becomeFollower(String nodeId, int replPort) {
            if (!leader.compareAndSet(true, false)) {
                throw new RuntimeException("Already follower");
            }
            String host = cluster.node(nodeId).hostAddress();
            InetSocketAddress replAddress = new InetSocketAddress(host, replPort);
            ReplicatedLog.this.replicationClient = new ReplicationClient(nodeId, replAddress, 100, 500, log, lastSequence.get());
            //become follower
        }

        @Override
        public void truncateLog(String nodeId) {

        }

        @Override
        public long getCommitIndex() {
            return log.entries();
        }
    }

    private class SequenceNaming implements NamingStrategy {

        private final int digits = (int) (Math.log10(Long.MAX_VALUE) + 1);

        @Override
        public String prefix() {
            return format("%0" + digits + "d", lastSequence.get());
        }
    }
}
