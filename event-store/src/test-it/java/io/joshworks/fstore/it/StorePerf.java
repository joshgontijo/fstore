package io.joshworks.fstore.it;

import io.joshworks.fstore.EventStore;
import io.joshworks.fstore.api.EventStoreIterator;
import io.joshworks.fstore.core.metrics.Metrics;
import io.joshworks.fstore.core.util.TestUtils;
import io.joshworks.fstore.core.util.Threads;
import io.joshworks.fstore.es.shared.EventMap;
import io.joshworks.fstore.es.shared.EventRecord;
import io.joshworks.fstore.serializer.json.JsonSerializer;

import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class StorePerf {

    private static final String STREAM_PREFIX = "stream-";
    private static final int ITEMS = 15000000;
    private static final int STREAMS = 50000;
    private static final int THREADS = 1;
    private static final int LOG_INTERVAL = 10000;

    private static final Metrics metrics = new Metrics();
    private static final AtomicBoolean monitoring = new AtomicBoolean(true);

    public static void main(String[] args) throws Exception {

        EventStore store = EventStore.open(TestUtils.testFolder());

        System.out.println("----------------- WRITE ---------------");
        ExecutorService executor = Executors.newFixedThreadPool(THREADS);

        for (int i = 0; i < THREADS; i++) {
            executor.submit(() -> write(store));
        }

        Thread monitor = new Thread(StorePerf::monitor);
        monitor.start();

        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.DAYS);


//        System.out.println("----------------- READ ---------------");
//        NodeClientIterator iterator = storeClient.iterator(50, "stream-2", "stream-4");
//        iterateAll(iterator);

        System.out.println("----------------- READ ALL ---------------");
        EventStoreIterator iterator = store.fromStreams(EventMap.empty(), Set.of(STREAM_PREFIX + "*"));
        iterateAll(iterator);

        monitoring.set(false);
        iterator.close();
        store.close();

    }

    private static void write(EventStore store) {
        long s = System.currentTimeMillis();
        byte[] data = JsonSerializer.toBytes(new UserCreated("josh", 123));
        for (int i = 0; i < ITEMS; i++) {
            String stream = STREAM_PREFIX + (i % STREAMS);

            long start = System.currentTimeMillis();
            store.append(EventRecord.create(stream, "USER_CREATED", data));
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
            long avgWriteTime = totalWrites == null ? 0 : totalWrites / writeTime;
            System.out.println("WRITE: " + writes + "/s - TOTAL WRITE: " + totalWrites + " - AVG_WRITE_TIME: " + avgWriteTime + " READ: " + reads + "/s - TOTAL READ: " + totalReads);
            Threads.sleep(1000);
        }
    }

    private static void iterateAll(EventStoreIterator iterator) {
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
