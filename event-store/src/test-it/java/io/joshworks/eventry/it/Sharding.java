package io.joshworks.eventry.it;

import io.joshworks.eventry.EventStore;
import io.joshworks.eventry.LinkToPolicy;
import io.joshworks.eventry.PartitionedStore;
import io.joshworks.eventry.Repartitioner;
import io.joshworks.eventry.StreamName;
import io.joshworks.eventry.SystemEventPolicy;
import io.joshworks.eventry.api.EventStoreIterator;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.partition.Partition;
import io.joshworks.eventry.partition.Partitions;
import io.joshworks.fstore.core.util.FileUtils;
import io.joshworks.fstore.core.util.Threads;
import io.joshworks.fstore.log.CloseableIterator;
import io.joshworks.fstore.log.iterators.Iterators;
import io.joshworks.fstore.serializer.kryo.KryoStoreSerializer;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Sharding {


    private static final String CA = "EV_CA";
    private static final String VP = "EV_VP";
    private static final String OC = "OC";

    private static final String[] EVENTS = {CA, VP, OC};
    private static final String[] PRODUCTS = IntStream.range(0, 50).boxed().map(i -> "PROD_" + i).toArray(String[]::new);
    private static final String[] USERS = IntStream.range(0, 50).boxed().map(i -> "USER_" + i).toArray(String[]::new);

    private static final ThreadLocalRandom random = ThreadLocalRandom.current();
    private static final AtomicBoolean stopped = new AtomicBoolean();
    private static final AtomicLong items = new AtomicLong();


    public static void main(String[] args) throws InterruptedException {
        File root = FileUtils.testFolder();
        File file1 = new File(root, "store-1");
        File file2 = new File(root, "store-2");
        File file3 = new File(root, "store-3");

        FileUtils.tryDelete(file1);
        FileUtils.tryDelete(file2);
        FileUtils.tryDelete(file3);

        int numPartitions = 3;
        String nodeId = "test-node";
        Partitions partitions = new Partitions(numPartitions, nodeId);
        partitions.add(new Partition(0, nodeId, EventStore.open(file1)));
        partitions.add(new Partition(1, nodeId, EventStore.open(file2)));
        partitions.add(new Partition(2, nodeId, EventStore.open(file3)));

        try (PartitionedStore store = new PartitionedStore(partitions)) {

            //stream by type
            Repartitioner byType = new Repartitioner(store, "USER_*", byType());
            Repartitioner byProd = new Repartitioner(store, "USER_*", byProduct());


            Thread report = new Thread(() -> {
                while (!stopped.get()) {
                    System.out.println("ITEMS: " + items.get() + " | BY-TYPE: " + byType.stats() + " | BY-PROD: " + byProd.stats());
                    Threads.sleep(2000);
                }
            });



            for (int event = 0; event < 1000; event++) {
                EventRecord ev = randEvent();
                System.out.println(ev);
                store.append(ev);
                items.incrementAndGet();
            }

            report.start();
            byType.run();
            byProd.run();

            Threads.sleep(120000);
            byProd.close();
            byType.close();
            stopped.set(true);
        }
    }


    private static Function<EventRecord, String> byType() {
        return r -> r.type;
    }

    private static Function<EventRecord, String> byProduct() {
        return r -> {
            Map<String, Object> data = KryoStoreSerializer.deserialize(r.body);
            return (String) data.get("product");
        };
    }

    private static EventRecord randEvent() {
        String user = USERS[random.nextInt(0, USERS.length)];
        String type = EVENTS[random.nextInt(0, EVENTS.length)];
        String product = PRODUCTS[random.nextInt(0, PRODUCTS.length)];
        return event(user, type, product);
    }

    private static CloseableIterator<EventRecord> fromStream(StreamName stream, EventStore... stores) {
        List<EventStoreIterator> its = Arrays.stream(stores).map(s -> s.fromStream(stream)).collect(Collectors.toList());
        return Iterators.ordered(its, er -> er.timestamp);
    }

    private static CloseableIterator<EventRecord> fromStreams(String prefix, EventStore... stores) {
        List<EventStoreIterator> its = Arrays.stream(stores).map(s -> s.fromStreams(prefix)).collect(Collectors.toList());
        return Iterators.ordered(its, er -> er.timestamp);
    }

    private static CloseableIterator<EventRecord> fromAll(EventStore... stores) {
        List<EventStoreIterator> its = Arrays.stream(stores).map(s -> s.fromAll(LinkToPolicy.INCLUDE, SystemEventPolicy.IGNORE)).collect(Collectors.toList());
        return Iterators.ordered(its, er -> er.timestamp);
    }

    private static EventRecord event(String stream, String type, String product) {
        byte[] data = KryoStoreSerializer.serialize(Map.of("product", product, "user", stream));
        return EventRecord.create(stream, type, data);
    }

    private static void linkTo(EventStore store, String stream, Function<EventRecord, String> func) {
        store.fromStream(StreamName.of(stream))
                .forEachRemaining(event -> {
                    Threads.sleep(1);
                    String targetStream = func.apply(event);
                    store.linkTo(targetStream, event);
                });
    }

    private static void print(EventStore store) {
        store.fromAll(LinkToPolicy.INCLUDE, SystemEventPolicy.IGNORE)
                .forEachRemaining(eventRecord -> System.out.println(eventRecord + " | " + new String(eventRecord.body)));
    }

    private static void print(CloseableIterator<EventRecord> iterator) {
        iterator.forEachRemaining(eventRecord -> System.out.println(eventRecord + " | " + new String(eventRecord.body)));
    }

}
