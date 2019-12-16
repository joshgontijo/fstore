package io.joshworks.lsm.server;

import io.joshworks.fstore.cluster.ClusterNode;
import io.joshworks.fstore.cluster.NodeInfo;
import io.joshworks.fstore.cluster.MulticastResponse;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.util.AttributeKey;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.tcp.TcpMessageServer;
import io.joshworks.fstore.tcp.server.ServerEventHandler;
import io.joshworks.lsm.server.events.NodeJoined;
import io.joshworks.lsm.server.events.NodeLeft;
import io.joshworks.lsm.server.handler.ReplicationHandler;
import io.joshworks.lsm.server.handler.TcpEventHandler;
import io.joshworks.lsm.server.partition.HashPartitioner;
import io.joshworks.lsm.server.partition.Partitioner;
import io.joshworks.lsm.server.replication.Replicas;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xnio.Options;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.Lock;

public class Server<K extends Comparable<K>> implements AutoCloseable {

    private static final String RING_LOCK = "RING_LOCK";
    private static final String RING_KEY = "RING_ID";

    private static final AttributeKey<io.joshworks.lsm.server.NodeInfo> NODE_INFO_KEY = AttributeKey.create(io.joshworks.lsm.server.NodeInfo.class);

    private static final Logger logger = LoggerFactory.getLogger(Server.class);

    private final ClusterNode clusterNode;
    private final Replicas replicas;
    private final NodeDescriptor descriptor;

    private final LsmCluster<K> clusterStore;

    private final List<StoreNode> remoteNodes = new CopyOnWriteArrayList<>();
    private final TcpMessageServer clientListener;
    private final TcpMessageServer replicationListener;

    private Server(
            File rootDir,
            ClusterNode clusterNode,
            NodeDescriptor descriptor,
            Partitioner partitioner,
            Serializer<K> keySerializer,
            io.joshworks.lsm.server.NodeInfo nodeInfo) {
        this.clusterNode = clusterNode;
        this.descriptor = descriptor;
        this.clusterStore = new LsmCluster<>(rootDir, keySerializer, remoteNodes, partitioner, nodeInfo);
        this.replicas = new Replicas(rootDir);
        this.clientListener = startListener(new TcpEventHandler(clusterStore), nodeInfo.tcpPort);
        this.replicationListener = startListener(new ReplicationHandler(replicas), nodeInfo.replicationPort);
    }


    public static <K extends Comparable<K>> Server<K> join(File rootDir, Serializer<K> serializer, String clusterName, int tcpPort, int replicationTcpPort) {
        NodeDescriptor descriptor = loadDescriptor(rootDir, clusterName);
        ClusterNode clusterNode = new ClusterNode(clusterName, descriptor.nodeId());
        logger.info("Joining cluster {}", clusterNode.name());
        clusterNode.join();
        logger.info("Joined cluster {}", clusterNode.name());

        Partitioner partitioner = new HashPartitioner();

        int ringId = descriptor.asInt(RING_KEY).orElseGet(() -> acquireRingId(clusterNode));

        NodeInfo cNode = clusterNode.node();
        //FIXME concurrency issue when connecting
        io.joshworks.lsm.server.NodeInfo thisNode = new io.joshworks.lsm.server.NodeInfo(cNode.id, ringId, cNode.hostAddress(), replicationTcpPort, tcpPort, Status.ACTIVE);
        clusterNode.node().attach(NODE_INFO_KEY, thisNode);

        Server<K> server = new Server<>(rootDir, clusterNode, descriptor, partitioner, serializer, thisNode);

        clusterNode.interceptor((msg, obj) -> logger.info("RECEIVED FROM {}: {}", msg.src(), obj));
        clusterNode.register(NodeJoined.class, server::nodeJoined);
        clusterNode.register(NodeLeft.class, server::nodeLeft);

        List<MulticastResponse> responses = clusterNode.client().cast(new NodeJoined(thisNode));
        for (MulticastResponse response : responses) {
            io.joshworks.lsm.server.NodeInfo nodeInfoInfo = response.message();
            logger.info("Received node info: {}", nodeInfoInfo);
            server.onNewNode(nodeInfoInfo);
        }

        return server;
    }

    private static int acquireRingId(ClusterNode clusterNode) {
        Lock lock = clusterNode.client().lock(RING_LOCK);
        lock.lock();
        try {

        } finally {
            lock.unlock();
        }

        throw new UnsupportedOperationException();
    }

    private static TcpMessageServer startListener(ServerEventHandler handler, int port) {
        return TcpMessageServer.create()
                .onOpen(conn -> System.out.println("Connection opened"))
                .onClose(conn -> System.out.println("Connection closed"))
                .onIdle(conn -> System.out.println("Connection idle"))
//                .idleTimeout(10, TimeUnit.SECONDS)
                .bufferSize(Size.KB.ofInt(4))
                .option(Options.REUSE_ADDRESSES, true)
                .option(Options.TCP_NODELAY, true)
                .option(Options.RECEIVE_BUFFER, Size.KB.ofInt(4))
                .option(Options.SEND_BUFFER, Size.KB.ofInt(4))
                .option(Options.WORKER_IO_THREADS, 1)
                .option(Options.WORKER_TASK_CORE_THREADS, 1)
                .option(Options.WORKER_TASK_MAX_THREADS, 1)
                .onEvent(handler)
                .start(new InetSocketAddress("localhost", port));
    }

    private static NodeDescriptor loadDescriptor(File rootDir, String clusterName) {
        NodeDescriptor descriptor = NodeDescriptor.read(rootDir);

        if (descriptor == null) {
            descriptor = NodeDescriptor.create(rootDir, clusterName);
        }
        if (!descriptor.clusterName().equals(clusterName)) {
            throw new IllegalArgumentException("Cannot connect store from cluster " + descriptor.clusterName() + " to another cluster: " + clusterName);
        }
        return descriptor;
    }

    private io.joshworks.lsm.server.NodeInfo nodeJoined(NodeJoined nodeJoined) {
        onNewNode(nodeJoined.nodeInfo);
        return clusterNode.node().get(NODE_INFO_KEY);
    }

    private void onNewNode(io.joshworks.lsm.server.NodeInfo nodeInfo) {
        remoteNodes.add(new RemoteNode(nodeInfo));
        logger.info("Node connected {}", nodeInfo);
        replicas.initialize(nodeInfo.id);
    }

    private void nodeLeft(NodeLeft nodeLeft) {
        remoteNodes.removeIf(node -> node.id().equals(nodeLeft.nodeId));
    }

    @Override
    public void close() {
        clusterNode.close();
        clusterStore.close();
        clientListener.close();
        replicationListener.close();
    }
}
