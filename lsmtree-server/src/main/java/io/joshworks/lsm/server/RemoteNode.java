package io.joshworks.lsm.server;

import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.tcp.TcpClientConnection;
import io.joshworks.fstore.tcp.TcpEventClient;
import io.joshworks.lsm.server.messages.Delete;
import io.joshworks.lsm.server.messages.Get;
import io.joshworks.lsm.server.messages.Put;
import io.joshworks.lsm.server.messages.Replicate;
import io.joshworks.lsm.server.messages.Result;
import org.xnio.Options;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

public class RemoteNode implements StoreNode {

    private final TcpClientConnection nodeClient;
    private final TcpClientConnection replicationClient;
    private final Node node;

    public RemoteNode(Node node) {
        this.node = node;
        this.nodeClient = createClient(node.tcp(), "tcp-client");
        this.replicationClient = createClient(node.replicationTcp(), "replication-client");
    }

    private TcpClientConnection createClient(InetSocketAddress address, String name) {
        return TcpEventClient.create()
                .option(Options.WORKER_NAME, name)
                .option(Options.WORKER_IO_THREADS, 1)
                .option(Options.TCP_NODELAY, true)
                .option(Options.SEND_BUFFER, Size.KB.ofInt(8))
                .maxEventSize(Size.KB.ofInt(32))
                .onClose(conn -> System.out.println("CLIENT: closing connection " + conn))
                .onEvent((connection, data) -> {
                    //do nothing
//                        System.out.println(data);
                })
                .connect(address, 5, TimeUnit.SECONDS);
    }

    @Override
    public void put(Put msg) {

    }

    @Override
    public void replicate(Replicate msg) {
        replicationClient.send(msg);
    }

    @Override
    public Result get(Get msg) {
        return null;
    }

    @Override
    public void delete(Delete msg) {

    }

    @Override
    public void close() {
        //do nothing
    }

    @Override
    public String id() {
        return node.id;
    }

}
