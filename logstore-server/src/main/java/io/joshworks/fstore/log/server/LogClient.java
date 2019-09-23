package io.joshworks.fstore.log.server;

import io.joshworks.eventry.network.tcp.TcpClientConnection;
import io.joshworks.eventry.network.tcp.client.TcpEventClient;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.server.tcp.Append;
import io.joshworks.fstore.serializer.json.JsonSerializer;
import org.xnio.Options;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;

public class LogClient {

    public static void main(String[] args) {

        TcpClientConnection client = TcpEventClient.create()
//                .keepAlive(2, TimeUnit.SECONDS)
//                .option(Options.SEND_BUFFER, Size.MB.ofInt(100))
                .option(Options.WORKER_IO_THREADS, 1)
                .option(Options.WORKER_TASK_CORE_THREADS, 1)
                .option(Options.WORKER_TASK_MAX_THREADS, 1)
                .bufferSize(Size.KB.ofInt(16))
                .option(Options.SEND_BUFFER, Size.KB.ofInt(16))
                .option(Options.RECEIVE_BUFFER, Size.KB.ofInt(16))
                .onEvent((connection, data) -> {
                    //TODO
                })
                .connect(new InetSocketAddress("localhost", 9999), 5, TimeUnit.SECONDS);


        long start = System.currentTimeMillis();
        for (int i = 0; i < 50000000; i++) {
            byte[] data = JsonSerializer.toBytes(new DummyEvent(String.valueOf(i), i));
            client.send(new Append(String.valueOf(i), data));
            if (i % 100000 == 0) {
                System.out.println("[" + Thread.currentThread().getName() + "] WRITE: " + i + " IN " + (System.currentTimeMillis() - start));
                start = System.currentTimeMillis();
            }
        }


    }
}
