package io.joshworks.lsm.server;

import io.joshworks.fstore.cluster.Cluster;
import io.joshworks.fstore.cluster.NodeInfo;
import io.joshworks.fstore.cluster.MulticastResponse;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.util.AttributeKey;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.tcp.TcpMessageServer;
import io.joshworks.fstore.tcp.server.ServerEventHandler;
import io.joshworks.lsm.server.events.NodeJoined;
import io.joshworks.lsm.server.events.NodeLeft;
import io.joshworks.lsm.server.events.AssignRingId;
import io.joshworks.lsm.server.events.RingId;
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

    private static final AttributeKey<Node> NODE_INFO_KEY = AttributeKey.create(Node.class);

    private static final Logger logger = LoggerFactory.getLogger(Server.class);

    private final Cluster cluster;
    private final Replicas replicas;
    private final NodeDescriptor descriptor;

    private final LsmCluster<K> clusterStore;

    private final List<StoreNode> remoteNodes = new CopyOnWriteArrayList<>();
    private final TcpMessageServer clientListener;
    private final TcpMessageServer replicationListener;

    private Server(
            File rootDir,
            Cluster cluster,
            NodeDescriptor descriptor,
            Partitioner partitioner,
            Serializer<K> keySerializer,
            Node node) {
        this.cluster = cluster;
        this.descriptor = descriptor;
        this.clusterStore = new LsmCluster<>(rootDir, keySerializer, remoteNodes, partitioner, node);
        this.replicas = new Replicas(rootDir);
        this.clientListener = startListener(new TcpEventHandler(clusterStore), node.tcpPort);
        this.replicationListener = startListener(new ReplicationHandler(replicas), node.replicationPort);
    }


    public static <K extends Comparable<K>> Server<K> join(File rootDir, Serializer<K> serializer, String clusterName, int tcpPort, int replicationTcpPort) {
        NodeDescriptor descriptor = loadDescriptor(rootDir, clusterName);
        Cluster cluster = new Cluster(clusterName, descriptor.nodeId());
        logger.info("Joining cluster {}", cluster.name());
        cluster.join();
        logger.info("Joined cluster {}", cluster.name());

        Partitioner partitioner = new HashPartitioner();

        int ringId = descriptor.asInt(RING_KEY).orElseGet(() -> acquireRingId(cluster));

        NodeInfo cNode = cluster.node();
        //FIXME concurrency issue when connecting
        Node thisNode = new Node(cNode.id, ringId, cNode.hostAddress(), replicationTcpPort, tcpPort, Status.ACTIVE);
        cluster.node().attach(NODE_INFO_KEY, thisNode);

        Server<K> server = new Server<>(rootDir, cluster, descriptor, partitioner, serializer, thisNode);

        cluster.interceptor((msg, obj) -> logger.info("RECEIVED FROM {}: {}", msg.src(), obj));
        cluster.register(NodeJoined.class, server::nodeJoined);
        cluster.register(NodeLeft.class, server::nodeLeft);
        cluster.register(AssignRingId.class, server::nodeLeft);

        List<MulticastResponse> responses = cluster.client().cast(new NodeJoined(thisNode));
        for (MulticastResponse response : responses) {
            Node nodeInfo = response.message();
            logger.info("Received node info: {}", nodeInfo);
            server.onNewNode(nodeInfo);
        }

        return server;
    }

    private static int acquireRingId(Cluster cluster) {
        Lock lock = cluster.client().lock(RING_LOCK);
        lock.lock();
        try {
            cluster.client().send(cluster.coordinator(), )

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

    private Node nodeJoined(NodeJoined nodeJoined) {
        onNewNode(nodeJoined.node);
        return cluster.node().get(NODE_INFO_KEY);
    }

    private void onNewNode(Node node) {
        remoteNodes.add(new RemoteNode(node));
        logger.info("Node connected {}", node);
        replicas.initialize(node.id);
    }

    private void nodeLeft(NodeLeft nodeLeft) {
        remoteNodes.removeIf(node -> node.id().equals(nodeLeft.nodeId));
    }

    private synchronized RingId ringIdRequested(AssignRingId request) {



    }

    @Override
    public void close() {
        cluster.close();
        clusterStore.close();
        clientListener.close();
        replicationListener.close();
    }
}
