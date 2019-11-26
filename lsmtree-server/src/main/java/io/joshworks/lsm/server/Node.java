package io.joshworks.lsm.server;

import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.tcp.TcpClientConnection;
import io.joshworks.fstore.tcp.client.TcpEventClient;
import org.xnio.Options;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

public class Node {

    private final TcpClientConnection nodeClient;
    private final TcpClientConnection replicationClient;
    private final NodeInfo nodeInfo;

    public Node(NodeInfo nodeInfo) {
        this.nodeInfo = nodeInfo;
        this.nodeClient = createClient(nodeInfo.tcp(), "tcp-client");
        this.replicationClient = createClient(nodeInfo.replicationTcp(), "replication-client");
    }

    private TcpClientConnection createClient(InetSocketAddress address, String name) {
        return TcpEventClient.create()
                .option(Options.WORKER_NAME, name)
                .option(Options.WORKER_IO_THREADS, 1)
                .option(Options.TCP_NODELAY, true)
                .option(Options.SEND_BUFFER, Size.KB.ofInt(8))
                .bufferSize(Size.KB.ofInt(32))
                .onClose(conn -> System.out.println("CLIENT: closing connection " + conn))
                .onEvent((connection, data) -> {
                    //do nothing
//                        System.out.println(data);
                })
                .connect(address, 5, TimeUnit.SECONDS);
    }

    public String id() {
        return nodeInfo.id;
    }

    public TcpClientConnection tcp() {
        return nodeClient;
    }

    public TcpClientConnection replication() {
        return replicationClient;
    }

    public NodeInfo info() {
        return nodeInfo;
    }

}
