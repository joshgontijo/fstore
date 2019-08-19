package io.joshworks.eventry.network;

import io.joshworks.eventry.network.tcp.TcpConnection;
import io.joshworks.eventry.network.tcp.TcpEventClient;
import io.joshworks.eventry.network.tcp.XTcpServer;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.core.util.Threads;
import org.xnio.Options;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class TcpTest {

    private static final String HOST = "localhost";
    private static final int PORT = 12345;

    private static final int ITEMS = 10000000;


    private static final AtomicLong sent = new AtomicLong();
    private static final AtomicLong received = new AtomicLong();

    public static void main(String[] args) throws InterruptedException {

        XTcpServer server = XTcpServer.create()
                .onOpen(conn -> System.out.println("SERVER: Connection opened"))
                .onClose(conn -> System.out.println("SERVER: Connection closed"))
                .onIdle(conn -> System.out.println("SERVER: Connection idle"))
//                .idleTimeout(10, TimeUnit.SECONDS)
                .bufferSize(Size.MB.ofInt(5))
                .option(Options.WORKER_NAME, "server")
                .option(Options.WORKER_IO_THREADS, 1)
                .option(Options.RECEIVE_BUFFER, 8096 * 50)
                .onEvent((connection, data) -> {
                    received.incrementAndGet();
                })
                .start(new InetSocketAddress(HOST, PORT));


        TcpConnection client = TcpEventClient.create()
                .option(Options.WORKER_NAME, "client")
                .option(Options.WORKER_IO_THREADS, 1)
                .option(Options.TCP_NODELAY, true)
                .option(Options.SEND_BUFFER, 8096 * 50)
                .onEvent((connection, data) -> {
                    //do nothing
                })
                .connect(new InetSocketAddress(HOST, PORT), 5, TimeUnit.SECONDS);


        Thread clientThread = new Thread(() -> {
            long start = System.currentTimeMillis();
            for (int i = 0; i < ITEMS; i++) {
                client.send(new Payload(String.valueOf(i)));
                sent.incrementAndGet();
            }
            System.out.println("Sent in " + (System.currentTimeMillis() - start));
        });


        Thread monitor = new Thread(() -> {
            long received = 0;
            while (received < ITEMS) {
                received = server.printConnections().values().stream().mapToLong(TcpConnection::messagesReceived).sum();
                System.out.println(sent.get() + " -> " + client.messagesSent() + " / " + received);
                Threads.sleep(2000);
            }
        });


        Threads.sleep(2000);
        clientThread.start();
        monitor.start();

        monitor.join();
        clientThread.join();

        client.close();
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
