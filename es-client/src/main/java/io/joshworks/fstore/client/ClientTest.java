package io.joshworks.fstore.client;

import io.joshworks.fstore.es.shared.EventRecord;
import io.joshworks.fstore.es.shared.routing.HashRouter;

import java.net.InetSocketAddress;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ClientTest {

    private static final String STREAM_PREFIX = "stream-";
    private static final int ITEMS = 5000000;
    private static final int STREAMS = 1000000;
    private static final int LOG_INTERVAL = 10000;

    private static AtomicLong counter = new AtomicLong();

    public static void main(String[] args) throws Exception {

        StoreClient storeClient = StoreClient.connect(new HashRouter(), new InetSocketAddress("localhost", 10000));

        System.out.println("----------------- WRITE ---------------");
//        Thread t1 = new Thread(() -> write(storeClient));
//        Thread t2 = new Thread(() -> write(storeClient));
//        Thread t3 = new Thread(() -> write(storeClient));

//        t1.start();
//        t2.start();
//        t3.start();

//        t1.join();
//        t2.join();
//        t3.join();


//        System.out.println("----------------- READ ---------------");
//        NodeClientIterator iterator = storeClient.iterator(50, "stream-2", "stream-4");
//        iterateAll(iterator);

        System.out.println("----------------- READ ALL ---------------");
        NodeClientIterator iterator = storeClient.iterator(50, "stream-*");
        iterateAll(iterator);


        storeClient.close();

    }

    private static void write(StoreClient storeClient) {
        long start = System.currentTimeMillis();
        long s = System.currentTimeMillis();
        String threadName = Thread.currentThread().getName();
        for (int i = 0; i < ITEMS; i++) {
            String stream = STREAM_PREFIX + (i % STREAMS);
            storeClient.append(stream, "USER_CREATED", new UserCreated("josh", i));
            long total = counter.incrementAndGet();
            if (i % LOG_INTERVAL == 0) {
                System.out.println("[" + threadName + "] WRITE: " + i + " IN " + (System.currentTimeMillis() - start) + " - TOTAL: " + total);
                start = System.currentTimeMillis();
            }
        }
        System.out.println("TOTAL WRITES: " + ITEMS + " IN " + (System.currentTimeMillis() - s));
    }

    private static void iterateAll(NodeClientIterator iterator) {
        int i = 0;
        long start = System.currentTimeMillis();
        long s = System.currentTimeMillis();
        while (iterator.hasNext()) {
            EventRecord next = iterator.next();
//            System.out.println(next);
            if (i++ % LOG_INTERVAL == 0) {
                System.out.println("READ: " + i + " IN " + (System.currentTimeMillis() - start));
                start = System.currentTimeMillis();
            }
        }
        System.out.println("TOTAL READ: " + i + " in " + (System.currentTimeMillis() - s));
        iterator.close();
    }

    private static Set<String> evenStreams(int total) {
        return IntStream.range(0, total)
                .filter(i -> i % 2 == 0)
                .boxed()
                .map(i -> STREAM_PREFIX + i)
                .collect(Collectors.toSet());
    }
}
