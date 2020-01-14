package io.joshworks.fstore.network;

import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.core.util.Threads;
import io.joshworks.fstore.tcp.EventHandler;
import io.joshworks.fstore.tcp.TcpConnection;
import io.joshworks.fstore.tcp.TcpMessageServer;
import org.xnio.Options;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

public class TcpTest {

    public static final String HOST = "localhost";
    public static final int PORT = 12344;

    private static final int ITEMS = 1000000;
    private static final int CLIENTS = 1;

    private static final List<TcpConnection> clientConnections = new ArrayList<>();

    public static void main(String[] args) throws InterruptedException {

        TcpMessageServer server = TcpMessageServer.create()
//                .idleTimeout(10, TimeUnit.SECONDS)
                .maxEntrySize(Size.KB.ofInt(32))
                .option(Options.RECEIVE_BUFFER, Size.KB.ofInt(32))
                .option(Options.WORKER_NAME, "server")
                .option(Options.WORKER_IO_THREADS, 1)
                .option(Options.WORKER_TASK_MAX_THREADS, 1)
                .option(Options.TCP_NODELAY, true)
                .onEvent(new EventHandler() {
                    long count = 0;
                    @Override
                    public void onEvent(TcpConnection connection, Object data) {
                        if(count++ % 100000 == 0) {
                            System.out.println(count - 1);
                        }
                    }
                })
                .start(new InetSocketAddress(HOST, PORT));


//        Runnable sendTask = () -> {
//
//        };
//
//        ExecutorService executor = Executors.newFixedThreadPool(CLIENTS);
//        for (int i = 0; i < CLIENTS; i++) {
//            executor.submit(sendTask);
//        }


        Thread monitor = new Thread(() -> {
            long messageReceived = 0;
            long messageSent = 0;
            long bytesSent = 0;
            long bytesReceived = 0;
            while (true) {
                long prevReceived = messageReceived;
                long prevSent = messageSent;
                long prevBytesSent = bytesSent;
                long prevBytesReceived = bytesReceived;

                messageReceived = server.printConnections().values().stream().mapToLong(TcpConnection::messagesReceived).sum();
                bytesReceived = server.printConnections().values().stream().mapToLong(TcpConnection::bytesReceived).sum();

                messageSent = clientConnections.stream().mapToLong(TcpConnection::messagesSent).sum();
                bytesSent = clientConnections.stream().mapToLong(TcpConnection::bytesSent).sum();

                String mbSent = String.format("%.1f MB", ((float) bytesSent - prevBytesSent) / 1000000);
                String mbReceived = String.format("%.1f MB", ((float) bytesReceived - prevBytesReceived) / 1000000);

                System.out.println(messageSent + " / " + messageReceived + " -> " + (messageSent - prevSent) + " / " + (messageReceived - prevReceived) + " -> " + mbSent + " / " + mbReceived);
                Threads.sleep(2000);
            }
        });


//        monitor.start();
//        monitor.join();
//        executor.shutdown();

        server.awaitTermination();

        System.out.println("CLOSING SERVER");
        server.close();
    }


    private static class Payload {
        public final String data;

        private Payload(String message) {
            this.data = message;
        }

        @Override
        public String toString() {
            return "Payload{" + "data='" + data + '\'' +
                    '}';
        }
    }

}
