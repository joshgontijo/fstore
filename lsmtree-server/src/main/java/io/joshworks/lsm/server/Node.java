package io.joshworks.lsm.server;

import io.joshworks.fstore.tcp.TcpClientConnection;
import io.joshworks.fstore.tcp.client.TcpEventClient;
import io.joshworks.fstore.core.util.Size;
import org.xnio.Options;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

public class Node {

    private final TcpClientConnection client;
    private final NodeInfo nodeInfo;

    public Node(NodeInfo nodeInfo) {
        this.nodeInfo = nodeInfo;
        this.client = TcpEventClient.create()
                .option(Options.WORKER_NAME, "CLIENT-" + UUID.randomUUID().toString().substring(0, 3))
                .option(Options.WORKER_IO_THREADS, 1)
                .option(Options.TCP_NODELAY, true)
                .option(Options.SEND_BUFFER, Size.KB.ofInt(32))
                .bufferSize(Size.KB.ofInt(32))
                .onClose(conn -> System.out.println("CLIENT: closing connection " + conn))
                .onEvent((connection, data) -> {
                    //do nothing
//                        System.out.println(data);
                })
                .connect(nodeInfo.tcp(), 5, TimeUnit.SECONDS);
    }

    public String id() {
        return nodeInfo.id;
    }

    public TcpClientConnection tcp() {
        return client;
    }

    public NodeInfo node() {
        return nodeInfo;
    }

}
