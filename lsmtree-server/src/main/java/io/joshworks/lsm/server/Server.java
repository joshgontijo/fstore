package io.joshworks.lsm.server;

import io.joshworks.fstore.cluster.Cluster;
import io.joshworks.fstore.cluster.ClusterNode;
import io.joshworks.fstore.cluster.MulticastResponse;
import io.joshworks.fstore.core.util.AttributeKey;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.tcp.TcpMessageServer;
import io.joshworks.fstore.tcp.server.ServerEventHandler;
import io.joshworks.lsm.server.events.NodeJoined;
import io.joshworks.lsm.server.events.NodeLeft;
import io.joshworks.lsm.server.handler.ReplicationHandler;
import io.joshworks.lsm.server.handler.TcpEventHandler;
import io.joshworks.lsm.server.replication.Replicas;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xnio.Options;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class Server implements AutoCloseable {

    private static final String RING_LOCK = "RING_LOCK";
    private static final String RING_KEY = "RING_ID";

    private static final AttributeKey<NodeInfo> NODE_INFO_KEY = AttributeKey.create(NodeInfo.class);

    private static final Logger logger = LoggerFactory.getLogger(Server.class);
    private Replicas replicas;

    private final Cluster cluster;
    private final NodeDescriptor descriptor;

    private final LsmCluster clusterStore;

    private final List<Node> remoteNodes = new CopyOnWriteArrayList<>();
    private final TcpMessageServer clientListener;
    private final TcpMessageServer replicationListener;

    private Server(File rootDir, Cluster cluster, NodeDescriptor descriptor, int clientTcpPort, int replicationTcpPort) {
        this.cluster = cluster;
        this.descriptor = descriptor;
        this.clusterStore = new LsmCluster(rootDir, remoteNodes, cluster.nodeId());
        this.replicas = new Replicas(rootDir);
        this.clientListener = startListener(new TcpEventHandler(clusterStore), clientTcpPort);
        this.replicationListener = startListener(new ReplicationHandler(replicas), replicationTcpPort);
    }


    public static Server join(File rootDir, String clusterName, int tcpPort, int replicationTcpPort) {
        NodeDescriptor descriptor = loadDescriptor(rootDir, clusterName);
        Cluster cluster = new Cluster(clusterName, descriptor.nodeId());
        logger.info("Joining cluster {}", cluster.name());
        cluster.join();
        logger.info("Joined cluster {}", cluster.name());
        Server server = new Server(rootDir, cluster, descriptor, tcpPort, replicationTcpPort);

        cluster.interceptor((msg, obj) -> logger.info("RECEIVED FROM {}: {}", msg.src(), obj));
        cluster.register(NodeJoined.class, server::nodeJoined);
        cluster.register(NodeLeft.class, server::nodeLeft);

        int ringId = 123456;
        ClusterNode cNode = cluster.node();
        //FIXME concurrency issue when connecting
        NodeInfo thisNode = new NodeInfo(cNode.id, ringId, cNode.hostAddress(), replicationTcpPort, tcpPort, Status.ACTIVE);
        cluster.node().attach(NODE_INFO_KEY, thisNode);

        List<MulticastResponse> responses = cluster.client().cast(new NodeJoined(thisNode));
        for (MulticastResponse response : responses) {
            NodeInfo clusterNodeInfo = response.message();
            logger.info("Received node info: {}", clusterNodeInfo);
            server.onNewNode(clusterNodeInfo);
        }

        return server;
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

    private NodeInfo nodeJoined(NodeJoined nodeJoined) {
        onNewNode(nodeJoined.nodeInfo);
        return cluster.node().get(NODE_INFO_KEY);
    }

    private void onNewNode(NodeInfo nodeInfo) {
        remoteNodes.add(new Node(nodeInfo));
        logger.info("Node connected {}", nodeInfo);
        replicas.initialize(nodeInfo.id);
    }

    private void nodeLeft(NodeLeft nodeLeft) {
        remoteNodes.removeIf(node -> node.id().equals(nodeLeft.nodeId));
    }

    @Override
    public void close() {
        cluster.close();
        clusterStore.close();
        clientListener.close();
        replicationListener.close();
    }
}
