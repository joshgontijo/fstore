package io.joshworks.lsm.server;

import io.joshworks.eventry.network.Cluster;
import io.joshworks.eventry.network.tcp.TcpMessageServer;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.lsmtree.LsmTree;
import io.joshworks.fstore.serializer.Serializers;
import io.joshworks.lsm.server.events.NodeJoined;
import io.joshworks.lsm.server.events.NodeLeft;
import io.joshworks.lsm.server.handler.TcpEventHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xnio.Options;

import java.io.File;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class Server implements AutoCloseable {

    private static final Logger logger = LoggerFactory.getLogger(Server.class);

    private final Cluster cluster;
    private final NodeDescriptor descriptor;

    private final LsmCluster lsmTree;

    private final List<Node> nodes = new ArrayList<>();
    private final TcpMessageServer tcpMessageServer;

    private Server(File rootDir, Cluster cluster, NodeDescriptor descriptor, int tcpPort) {
        this.cluster = cluster;
        this.descriptor = descriptor;
        this.lsmTree = new LsmCluster(LsmTree.builder(rootDir, Serializers.VSTRING, Serializers.VLEN_BYTE_ARRAY)
                .sstableStorageMode(StorageMode.MMAP)
                .transactionLogStorageMode(StorageMode.MMAP)
                .open());
        this.tcpMessageServer = startTcpListener(lsmTree, tcpPort);
    }

    public static Server join(File rootDir, String clusterName, int tcpPort) {
        NodeDescriptor descriptor = loadDescriptor(rootDir, clusterName);
        Cluster cluster = new Cluster(clusterName, descriptor.nodeId());
        logger.info("Joining cluster {}", cluster);
        cluster.join();
        logger.info("Joined cluster {}", cluster);
        Server server = new Server(rootDir, cluster, descriptor, tcpPort);

        cluster.interceptor((msg, obj) -> logger.info("RECEIVED FROM {}: {}", msg.src(), obj));

        cluster.register(NodeJoined.class, server::nodeJoined);
        cluster.register(NodeLeft.class, server::nodeLeft);

        return server;
    }

    private static TcpMessageServer startTcpListener(LsmCluster lsmTree, int port) {
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
                .onEvent(new TcpEventHandler(lsmTree))
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

    private void nodeJoined(NodeJoined nodeJoined) {
        nodes.add(nodeJoined.node);
    }

    private void nodeLeft(NodeLeft nodeLeft) {
        nodes.removeIf(node -> node.id.equals(nodeLeft.nodeId));
    }

    @Override
    public void close() {
        cluster.close();
        lsmTree.close();
        tcpMessageServer.close();
    }
}
