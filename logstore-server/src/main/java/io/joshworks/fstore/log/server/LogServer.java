package io.joshworks.fstore.log.server;

import io.joshworks.eventry.network.tcp.TcpMessageServer;
import io.joshworks.fstore.core.util.FileUtils;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.server.tcp.TcpEventHandler;
import org.xnio.Options;

import java.io.File;
import java.net.InetSocketAddress;

public class LogServer {

    public static void main(String[] args) throws Exception {

        File root = new File("S:\\log-server");
        FileUtils.tryDelete(root);
        PartitionedLog store = new PartitionedLog(root, 3);

        TcpMessageServer tcpServer = TcpMessageServer.create()
                .onOpen(conn -> System.out.println("Connection opened"))
                .onClose(conn -> System.out.println("Connection closed"))
                .onIdle(conn -> System.out.println("Connection idle"))
//                .idleTimeout(10, TimeUnit.SECONDS)
                .bufferSize(Size.KB.ofInt(16))
                .option(Options.REUSE_ADDRESSES, true)
                .option(Options.TCP_NODELAY, true)
                .option(Options.RECEIVE_BUFFER, Size.KB.ofInt(16))
                .option(Options.WORKER_IO_THREADS, 3)
                .option(Options.WORKER_TASK_CORE_THREADS, 10)
                .option(Options.WORKER_TASK_MAX_THREADS, 10)
//                .option(Options.RECEIVE_BUFFER, Size.MB.ofInt(5))
                .onEvent(new TcpEventHandler(store))
                .start(new InetSocketAddress("localhost", 9999));

    }

}
