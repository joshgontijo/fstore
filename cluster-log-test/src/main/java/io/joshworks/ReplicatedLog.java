package io.joshworks;

import io.joshworks.fstore.cluster.ClusterNode;
import io.joshworks.fstore.cluster.MulticastResponse;
import io.joshworks.fstore.cluster.NodeInfo;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.log.appender.LogAppender;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class ReplicatedLog<T> implements Closeable {

    private final static String CLUSTER_NAME = "MY-CLUSTER";

    private final AtomicBoolean closed = new AtomicBoolean();
    private final LogAppender<ByteBuffer> log;
    private final ClusterNode node;
    private final String nodeId;
    private final int clusterSize; //TODO update dynamically
    private final AtomicBoolean leader = new AtomicBoolean();

    private final Coordinator coordinator = new Coordinator();

    //Only used by leader node
    private final Map<String, AtomicLong> commitIndexes = new ConcurrentHashMap<>();

    private TcpReplication<T> replicationServer;
    private Thread replicationClient;


    public ReplicatedLog(File root, Serializer<T> serializer, String nodeId, int clusterSize) {
        this.nodeId = nodeId;
        this.clusterSize = clusterSize;
        this.log = LogAppender.builder(new File(root, nodeId), new EntrySerializer<>(serializer)).open();

        commitIndexes.put(nodeId, new AtomicLong());
        commitIndexes.get(nodeId).set(log.entries());

        InetSocketAddress replPort = new InetSocketAddress("localhost", randomPort());
        this.replicationServer = new TcpReplication<>(replPort, log);

        this.node = new ClusterNode(CLUSTER_NAME, nodeId);
        this.node.registerRpcProxy(ClusterRpc.class);
        this.node.onNodeUpdated(this::onNodeConnected);
        this.node.join();

        //TODO start only for followers
        this.replicationClient = new Thread(new ReplicationTask<>(, 100, log));

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

    public void append(T value) {
        if (!hasMajority()) {
            throw new RuntimeException("Write rejected: No quorum");
        }
        if (!leader()) {
            //TODO do something smarter ?
            throw new RuntimeException("Not a leader");
        }

        System.out.println("[" + nodeId + "] Append " + value);

        long idx = log.entries();
        log.append(new Entry<>(idx, value));
        List<MulticastResponse> response = node.client().cast(value);
//        Address[] replicas = response.stream().map(MulticastResponse::address).toArray(Address[]::new);
        System.out.println("[" + nodeId + "] Replicated (" + value + ") to " + response.size() + " replicas");
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
        }

        @Override
        public void becomeFollower(String nodeId) {
            if (!leader.compareAndSet(true, false)) {
                throw new RuntimeException("Already follower");
            }
            //become follower
        }

        @Override
        public void truncateLog(String nodeId) {

        }

        @Override
        public long getCommitIndex() {
            return 0;
        }
    }


}
