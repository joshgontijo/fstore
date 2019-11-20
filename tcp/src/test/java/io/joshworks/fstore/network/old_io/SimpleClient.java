package io.joshworks.fstore.network.old_io;

import io.joshworks.fstore.core.util.Threads;

import java.io.InputStream;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicLong;

public class SimpleClient {

    private static final AtomicLong totalRead = new AtomicLong();

    public static void main(String[] args) throws Exception {
        Socket socket = new Socket("127.0.0.1", 6666);
        InputStream input = socket.getInputStream();

        startMonitor();

        byte[] bytes = new byte[32 * 1024]; // 32K
        for (int i = 1; ; i++) {
            int read = input.read(bytes);
            if (read < 0) break;
            totalRead.addAndGet(read);
        }
    }

    private static void startMonitor() throws InterruptedException {
        Thread monitor = new Thread(() -> {
            long bytesReceived = 0;
            long prevBytesReceived = 0;
            while (true) {
                bytesReceived = totalRead.get();
                String diff = String.format("%.1f MB", ((float) bytesReceived - prevBytesReceived) / 1000000);
                String rec = String.format("%.1f MB", ((float) bytesReceived) / 1000000);
                System.out.println("RECEIVED: " + rec + " diff " + diff);
                prevBytesReceived = bytesReceived;
                totalRead.set(0);
                Threads.sleep(2000);
            }
        });


        monitor.start();
    }

}