package io.joshworks;

import io.joshworks.fstore.cluster.Cluster;
import io.joshworks.fstore.cluster.NodeInfo;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.appender.LogAppender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class ReplicatedLog implements Closeable {

    public static final int NO_SEQUENCE = -1;

    private static final Logger logger = LoggerFactory.getLogger(ReplicatedLog.class);
    private final static String CLUSTER_NAME = "MY-CLUSTER";

    private final AtomicBoolean closed = new AtomicBoolean();
    private final LogAppender<Record> log;
    private final Cluster cluster;
    private final String nodeId;
    private final int clusterSize; //TODO update dynamically
    private final AtomicBoolean leader = new AtomicBoolean();
    private String following;

    private final CommitTable commitTable;
    private final Coordinator coordinator;

    //Only used by leader node
    private final int replPort;

    private ReplicationServer replicationServer;
    private ReplicationClient replicationClient;

    private static CountDownLatch ready = new CountDownLatch(1);


    public ReplicatedLog(File root, String nodeId, int clusterSize) {
        this.nodeId = nodeId;

        this.commitTable = new CommitTable();

        this.clusterSize = clusterSize;
        this.log = LogAppender.builder(new File(root, nodeId), new EntrySerializer())
//                .namingStrategy(new SequenceNaming())
                .open();


        commitTable.update(nodeId, findLastSequence());

        //FIXME: cannot set last commit index before checking with other nodes
//        commitIndexes.get(nodeId).set(lastSequence.get());

        this.replPort = randomPort();

        this.cluster = new Cluster(CLUSTER_NAME, nodeId);
        this.coordinator = new Coordinator(cluster, commitTable);
        this.cluster.registerRpcProxy(ClusterRpc.class);
        this.cluster.registerRpcHandler(new ClusterRpcHandler());
        this.cluster.onNodeUpdated(this::onNodeConnected);
        this.cluster.join();

        //Removed because all nodes are running locally
//        try {
//            logger.info("Waiting to join cluster");
//            ready.await(10, TimeUnit.SECONDS);
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }

    }

    private long findLastSequence() {
        try (LogIterator<Record> iterator = log.iterator(Direction.BACKWARD)) {
            return iterator.hasNext() ? iterator.next().sequence : NO_SEQUENCE;
        }
    }

    public boolean leader() {
        return leader.get();
    }

    //basic implementation: return only when all nodes are in sync
    public void append(ByteBuffer data, ReplicationLevel writeLevel, Consumer<Long> onComplete) {
        if (!hasQuorum()) {
            throw new RuntimeException("Write rejected: No quorum");
        }
        if (!leader()) {
            //TODO do something smarter ?
            throw new RuntimeException("Not a leader");
        }

//        System.out.println("[" + nodeId + "] Append");

        long idx = log.entries();
        long recordPos = log.append(new Record(idx, data));

        commitTable.update(nodeId, idx);
        WorkItem<Long> workItem = replicationServer.waitForReplication(writeLevel, idx);
        if (onComplete != null) {
            workItem.thenAccept(onComplete);
        }
    }

    public LogIterator<Record> iterator(Direction direction) {
        LogIterator<Record> iterator = log.iterator(direction);
        return new ReplicatedIterator(nodeId, commitTable, iterator);
    }

    public synchronized void onNodeConnected(NodeInfo newNode) {
        if (!cluster.isCoordinator()) {
            return;
        }

        if (!hasQuorum()) {
            logger.info("Not enough nodes for a quorum, no action...");
            return;
        }

        ClusterRpc proxy = cluster.rpcProxy(newNode.address);
        long commitIndex = proxy.getCommitIndex();
        commitTable.update(newNode.id, commitIndex);

        //TODO make atomic
        ClusterRpc newNodeProxy = cluster.rpcProxy(newNode.address);
        String currLeader = coordinator.leader();
        if (coordinator.tryLeaderShip(newNode.id)) {
            logger.info("Electing new leader: {}", newNode.id);
            int newLeaderReplPort = newNodeProxy.replicationPort();
            newNodeProxy.becomeLeader();
            for (NodeInfo node : cluster.nodes()) { //TODO use cluster group when using multiple namespaces
                if (!node.id.equals(newNode.id)) {
                    logger.info("Making {} follower of {}", node.id, newNode.id);
                    ClusterRpc nProxy = cluster.rpcProxy(node.address);
                    nProxy.becomeFollower(newNode.id, newLeaderReplPort);
                }
            }

        } else {
            ClusterRpc currLeaderProxy = cluster.rpcProxy(cluster.node(currLeader).address);
            logger.info("Making {} follower of {}", newNode.id, currLeader);
            int currLeaderReplPort = currLeaderProxy.replicationPort();
            newNodeProxy.becomeFollower(currLeader, currLeaderReplPort);
        }

        newNodeProxy.becomeAvailable();
    }

    public boolean hasQuorum() {
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

    @Override
    public String toString() {
        return "ReplicatedLog{" +
                " nodeId='" + nodeId + '\'' +
                " isLeader='" + leader() + '\'' +
                " isCoordinator='" + cluster.isCoordinator() + '\'' +
                " leader='" + following + '\'' +
                " sequence='" + commitTable.get(nodeId) + '\'' +
                '}';
    }

    private class ClusterRpcHandler implements ClusterRpc {

        @Override
        public void becomeLeader() {
            if (!leader.compareAndSet(false, true)) {
                throw new RuntimeException("Already leader");
            }
            if (ReplicatedLog.this.replicationClient != null) {
                ReplicatedLog.this.replicationClient.close();
            }
            //starts replication server
            InetSocketAddress replPort = new InetSocketAddress("localhost", ReplicatedLog.this.replPort);
            ReplicatedLog.this.replicationServer = new ReplicationServer(replPort, log, clusterSize, commitTable);
            logger.info("Started replication server for node {}", nodeId);
            following = null;
        }

        @Override
        public void becomeFollower(String leader, Integer replPort) {
            if (ReplicatedLog.this.replicationServer != null) {
                ReplicatedLog.this.replicationServer.close();
            }
            String host = cluster.node(leader).hostAddress();
            InetSocketAddress replAddress = new InetSocketAddress(host, replPort);

            long commitIndex = commitTable.get(ReplicatedLog.this.nodeId);
            ReplicatedLog.this.replicationClient = new ReplicationClient(ReplicatedLog.this.nodeId, replAddress, commitTable, 100, 500, log, commitIndex);
            following = leader;
        }

        @Override
        public void becomeAvailable() {
            logger.info("Node {} available", nodeId);
//            ready.countDown();
        }

        @Override
        public void truncateLog(String nodeId) {

        }

        @Override
        public long getCommitIndex() {
            return log.entries();
        }

        @Override
        public int replicationPort() {
            return replPort;
        }

    }

//    private class SequenceNaming implements NamingStrategy {
//
//        private final int digits = (int) (Math.log10(Long.MAX_VALUE) + 1);
//
//        @Override
//        public String prefix() {
//            long commitIndex = log.entries(); //sequence
//            return String.format("%0" + digits + "d", commitIndex);
//        }
//    }
}
