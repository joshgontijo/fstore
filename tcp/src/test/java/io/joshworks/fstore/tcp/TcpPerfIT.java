package io.joshworks.fstore.tcp;

import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.core.util.Threads;
import org.xnio.Options;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TcpPerfIT {

    public static final String HOST = "localhost";
    public static final int PORT = 12344;

    private static final int ITEMS = Integer.MAX_VALUE;
    private static final int CLIENTS = 1;

    private static final List<TcpConnection> clientConnections = new ArrayList<>();

    public static void main(String[] args) throws InterruptedException {

        TcpEventServer server = TcpEventServer.create()
                .idleTimeout(10, TimeUnit.SECONDS)
                .maxMessageSize(Size.KB.ofInt(32))
                .option(Options.RECEIVE_BUFFER, Size.KB.ofInt(32))
                .option(Options.WORKER_NAME, "server")
                .option(Options.WORKER_IO_THREADS, 1)
                .option(Options.TCP_NODELAY, true)
//                .onEvent(TypedEventHandler.builder().build())
                .start(new InetSocketAddress(HOST, PORT));


        Runnable sendTask = () -> {
            TcpConnection client = TcpEventClient.create()
                    .option(Options.WORKER_NAME, "CLIENT-" + UUID.randomUUID().toString().substring(0, 3))
                    .option(Options.WORKER_IO_THREADS, 1)
                    .option(Options.TCP_NODELAY, true)
                    .option(Options.SEND_BUFFER, Size.KB.ofInt(32))
                    .maxMessageSize(Size.KB.ofInt(32))
                    .onClose(conn -> System.out.println("CLIENT: closing connection " + conn))
                    .onEvent((connection, data) -> {
                        //do nothing
                    })
                    .connect(new InetSocketAddress(HOST, PORT), 5, TimeUnit.SECONDS);
            clientConnections.add(client);
            long start = System.currentTimeMillis();
            byte[] bytes = new byte[256];
            Arrays.fill(bytes, (byte) 1);
            ByteBuffer wrap = ByteBuffer.wrap(bytes);
            for (int i = 0; i < ITEMS; i++) {
                client.send(wrap, true);
                wrap.clear();
            }
            System.out.println("COMPLETED IN " + (System.currentTimeMillis() - start));
            Threads.sleep(100000);
//            client.close();
        };

        ExecutorService executor = Executors.newFixedThreadPool(CLIENTS);
        for (int i = 0; i < CLIENTS; i++) {
            executor.submit(sendTask);
        }


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


        monitor.start();
        monitor.join();
        executor.shutdown();

        System.out.println("CLOSING SERVER");
//        server.close();
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
