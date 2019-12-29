package io.joshworks;

import io.joshworks.fstore.cluster.ClusterNode;
import io.joshworks.fstore.cluster.NodeInfo;
import io.joshworks.fstore.log.appender.LogAppender;

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

public class ReplicatedLog implements Closeable {

    private final static String CLUSTER_NAME = "MY-CLUSTER";

    private final AtomicBoolean closed = new AtomicBoolean();
    private final LogAppender<Record> log;
    private final ClusterNode node;
    private final String nodeId;
    private final int clusterSize; //TODO update dynamically
    private final AtomicBoolean leader = new AtomicBoolean();

    private final Coordinator coordinator = new Coordinator();

    //Only used by leader node
    private final Map<String, AtomicLong> commitIndexes = new ConcurrentHashMap<>();

    private ReplicationServer replicationServer;
    private ReplicationClient replicationClient;


    public ReplicatedLog(File root, String nodeId, int clusterSize) {
        this.nodeId = nodeId;
        this.clusterSize = clusterSize;
        this.log = LogAppender.builder(new File(root, nodeId), new EntrySerializer()).open();

        commitIndexes.put(nodeId, new AtomicLong());
        commitIndexes.get(nodeId).set(log.entries());

        InetSocketAddress replPort = new InetSocketAddress("localhost", randomPort());
        this.replicationServer = new ReplicationServer(replPort, log);

        this.node = new ClusterNode(CLUSTER_NAME, nodeId);
        this.node.registerRpcProxy(ClusterRpc.class);
        this.node.onNodeUpdated(this::onNodeConnected);
        this.node.join();

    }

    private synchronized void onNodeConnected(NodeInfo nodeInfo) {
        if (!node.isCoordinator()) {
            return;
        }

        if (!hasMajority()) {
            System.out.println("Not enough nodes for a quorum, no action...");
            return;
        }

        coordinator.electLeader();

        ClusterRpc proxy = node.rpcProxy(nodeInfo.address);
        long commitIndex = proxy.getCommitIndex();
        commitIndexes.put(nodeInfo.id, new AtomicLong());
        commitIndexes.get(nodeInfo.id).set(commitIndex);

    }

    public boolean leader() {
        return leader.get();
    }

    //basic implementation: return only when all nodes are in sync
    public long append(ByteBuffer data) {
        if (!hasMajority()) {
            throw new RuntimeException("Write rejected: No quorum");
        }
        if (!leader()) {
            //TODO do something smarter ?
            throw new RuntimeException("Not a leader");
        }

        System.out.println("[" + nodeId + "] Append");

        long idx = log.entries();
        long recordPos = log.append(new Record(idx, data));
        commitIndexes.get(nodeId).set(idx);
    }

    private boolean hasMajority() {
        return node.numNodes() >= (clusterSize / 2) + 1;
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        node.close();
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
            //become leader
            ReplicatedLog.this.replicationClient.close();
        }

        @Override
        public void becomeFollower(String nodeId, int replPort) {
            if (!leader.compareAndSet(true, false)) {
                throw new RuntimeException("Already follower");
            }
            String host = node.node(nodeId).hostAddress();
            InetSocketAddress replAddress = new InetSocketAddress(host, replPort);
            ReplicatedLog.this.replicationClient = new ReplicationClient(nodeId, replAddress, 100, 500, log);
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


}
