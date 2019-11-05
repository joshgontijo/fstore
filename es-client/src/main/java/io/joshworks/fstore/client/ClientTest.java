package io.joshworks.fstore.client;

import io.joshworks.fstore.core.metrics.Metrics;
import io.joshworks.fstore.core.util.Threads;
import io.joshworks.fstore.es.shared.EventRecord;
import io.joshworks.fstore.es.shared.routing.HashRouter;

import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ClientTest {

    private static final String STREAM_PREFIX = "stream-";
    private static final int ITEMS = 5000000;
    private static final int STREAMS = 50000;
    private static final int THREADS = 1;

    private static final Metrics metrics = new Metrics();
    private static final AtomicBoolean monitoring = new AtomicBoolean(true);

    public static void main(String[] args) throws Exception {

        StoreClient storeClient = StoreClient.connect(new HashRouter(), new InetSocketAddress("localhost", 10000));

        System.out.println("----------------- WRITE ---------------");
        ExecutorService executor = Executors.newFixedThreadPool(THREADS);

//        for (int i = 0; i < THREADS; i++) {
//            executor.submit(() -> write(storeClient));
//        }

        Thread monitor = new Thread(ClientTest::monitor);
        monitor.start();

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.DAYS);



//        System.out.println("----------------- READ ---------------");
//        NodeClientIterator iterator = storeClient.iterator(50, "stream-2", "stream-4");
//        iterateAll(iterator);

//        System.out.println("----------------- READ ALL ---------------");
//        NodeClientIterator iterator = storeClient.iterator(50, STREAM_PREFIX + "*");
//        iterateAll(iterator);

        System.out.println("----------------- READ ALL ---------------");
        readAll(storeClient);
//        get(storeClient);

        monitoring.set(false);
        storeClient.close();

    }

    private static void write(StoreClient storeClient) {
        long s = System.currentTimeMillis();
        for (int i = 0; i < ITEMS; i++) {
            String stream = STREAM_PREFIX + (i % STREAMS);

            long start = System.currentTimeMillis();
            storeClient.append(stream, "USER_CREATED", new UserCreated("josh", i));
            metrics.update("writeTime", (System.currentTimeMillis() - start));
            metrics.update("writes");
            metrics.update("totalWrites");
        }
        System.out.println("WRITES: " + ITEMS + " IN " + (System.currentTimeMillis() - s));
    }

    private static void monitor() {
        while (monitoring.get()) {
            Long writes = metrics.remove("writes");
            Long reads = metrics.remove("reads");
            Long totalWrites = metrics.get("totalWrites");
            Long totalReads = metrics.get("totalReads");
            Long writeTime = metrics.get("writeTime");

            Long streamReads = metrics.remove("readStream");
            Long totalReadStream = metrics.get("totalReadStream");

            long avgWriteTime = totalWrites == null ? 0 : totalWrites / writeTime;
            System.out.println("WRITE: " + writes + "/s - TOTAL WRITE: " + totalWrites + " - AVG_WRITE_TIME: " + avgWriteTime + " READ: " + reads + "/s - TOTAL READ: " + totalReads + " STREAM_READ: " + streamReads + "/s - TOTAL_STREAM_READ: " + totalReadStream);
            Threads.sleep(1000);
        }
    }

    private static void readAll(StoreClient client) {
        for (int i = 0; i < ITEMS; i++) {
            String stream = STREAM_PREFIX + (i % STREAMS);
            List<EventRecord> records = client.readStream(stream, 0, 10);
            metrics.update("readStream");
            metrics.update("totalReadStream");
        }
    }

    private static void get(StoreClient client) {
        for (int i = 0; i < ITEMS; i++) {
            String stream = STREAM_PREFIX + (i % STREAMS);
            EventRecord records = client.get(stream, 0);
            metrics.update("readStream");
            metrics.update("totalReadStream");
        }
    }

    private static void iterateAll(NodeClientIterator iterator) {
        int i = 0;
        long start = System.currentTimeMillis();
        long s = System.currentTimeMillis();
        while (iterator.hasNext()) {
            EventRecord next = iterator.next();
//            System.out.println(next);
            metrics.update("reads");
            metrics.update("totalReads");
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

    public static class UserCreated {

        public final String name;
        public final int age;


        public UserCreated(String name, int age) {
            this.name = name;
            this.age = age;
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer("UserCreated{");
            sb.append("name='").append(name).append('\'');
            sb.append(", age=").append(age);
            sb.append('}');
            return sb.toString();
        }
    }

}
