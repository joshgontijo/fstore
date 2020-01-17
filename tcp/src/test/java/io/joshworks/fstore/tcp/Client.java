package io.joshworks.fstore.tcp;

import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.core.util.Threads;
import org.xnio.Options;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static io.joshworks.fstore.tcp.TcpTest.HOST;

public class Client {
    private static final int ITEMS = 100000000;

    public static void main(String[] args) {

        TcpConnection client = TcpEventClient.create()
                .option(Options.WORKER_NAME, "CLIENT-" + UUID.randomUUID().toString().substring(0, 3))
                .option(Options.WORKER_IO_THREADS, 1)
                .keepAlive(1, TimeUnit.SECONDS)
                .option(Options.TCP_NODELAY, true)
                .option(Options.SEND_BUFFER, Size.KB.ofInt(32))
                .maxMessageSize(Size.KB.ofInt(32))
                .onClose(conn -> System.out.println("CLIENT: closing connection " + conn))
                .onEvent((connection, data) -> {
                    //do nothing
                })
                .connect(new InetSocketAddress(HOST, 12344), 5, TimeUnit.SECONDS);
        long start = System.currentTimeMillis();

        byte[] data = new byte[256];
        Arrays.fill(data, (byte) 1);
        ByteBuffer buffer = ByteBuffer.wrap(data);

        for (int i = 0; i < ITEMS; i++) {
            client.sendAndFlush(buffer);
            buffer.clear();
            if (i % 100000 == 0) {
                System.out.println("SENT: " + i);
            }
        }
        System.out.println("COMPLETED IN " + (System.currentTimeMillis() - start));
        Threads.sleep(1000100);
        client.close();

    }
}
