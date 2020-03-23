package io.joshworks.fstore.tcp;

import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.core.util.Threads;
import org.xnio.Options;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TcpPerfIT3 {

    public static final String HOST = "localhost";
    public static final int PORT = 12344;

    private static final int ITEMS = Integer.MAX_VALUE;
    private static final int CLIENTS = 1;

    private static long bytesSent;
    private static long bytesReceived;

    public static void main(String[] args) throws Exception {

        ServerSocketChannel serverSocket = ServerSocketChannel.open();
        serverSocket.bind(new InetSocketAddress("localhost", PORT));

        Thread serverThread = new Thread(() -> {
            int read;
            try {
                SocketChannel clientSocket = serverSocket.accept();
                var buffer = ByteBuffer.allocate(4096);
                while ((read = clientSocket.read(buffer)) != -1) {
                    bytesReceived += read;
                    buffer.clear();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        serverThread.setName("server");
        serverThread.start();

        Threads.sleep(2000);

        Thread clientThread = new Thread(() -> {
            SocketChannel client = null;
            try {
                client = SocketChannel.open(new InetSocketAddress(HOST, PORT));
                long start = System.currentTimeMillis();
                byte[] bytes = new byte[256];
                Arrays.fill(bytes, (byte) 1);
                ByteBuffer wrap = ByteBuffer.wrap(bytes);
                for (int i = 0; i < ITEMS; i++) {
                    bytesSent += wrap.remaining();
                    client.write(wrap);
                    wrap.clear();
                }
                System.out.println("COMPLETED IN " + (System.currentTimeMillis() - start));
                Threads.sleep(100000);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });

        clientThread.setName("client");
        clientThread.start();


        Thread monitor = new Thread(() -> {
            long prevBytesSent = 0;
            long prevBytesReceived = 0;
            while (true) {

                String mbSent = String.format("%.1f MB", ((float) bytesSent - prevBytesSent) / 1000000);
                String mbReceived = String.format("%.1f MB", ((float) bytesReceived - prevBytesReceived) / 1000000);

                prevBytesSent = bytesSent;
                prevBytesReceived = bytesReceived;

                System.out.println(mbSent + " / " + mbReceived);
                Threads.sleep(2000);
            }
        });


        monitor.start();
        monitor.join();
    }

}
