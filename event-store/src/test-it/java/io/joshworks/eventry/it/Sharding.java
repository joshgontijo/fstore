package io.joshworks.eventry.it;

import io.joshworks.eventry.EventLogIterator;
import io.joshworks.eventry.EventStore;
import io.joshworks.eventry.LinkToPolicy;
import io.joshworks.eventry.Repartitioner;
import io.joshworks.eventry.StreamIterator;
import io.joshworks.eventry.StreamName;
import io.joshworks.eventry.SystemEventPolicy;
import io.joshworks.eventry.log.EventRecord;
import io.joshworks.fstore.core.util.FileUtils;
import io.joshworks.fstore.core.util.Threads;
import io.joshworks.fstore.log.CloseableIterator;
import io.joshworks.fstore.log.iterators.Iterators;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Sharding {


    private static final String CA = "EV_CA";
    private static final String VP = "EV_VP";
    private static final String OC = "OC";

    public static void main(String[] args) throws InterruptedException {
        File file1 = new File("D:\\ess\\store-1");
        File file2 = new File("D:\\ess\\store-2");

        FileUtils.tryDelete(file1);
        FileUtils.tryDelete(file2);

        try (EventStore store1 = EventStore.open(file1);
             EventStore store2 = EventStore.open(file2)) {

            events("user-1", "prod-1").forEach(store1::append);
            events("user-1", "prod-2").forEach(store1::append);
            events("user-2", "prod-1").forEach(store2::append);


            new Repartitioner(store1, "user-1", ev -> ev.type);
            new Repartitioner(store2, "user-2", ev -> ev.type);


            final CloseableIterator<EventRecord> ordered = fromStreams("EV_", store1, store2);
            new Thread(() -> {
                while (true) {
                    System.out.println("------ CAs ------");
                    print(ordered);

                    Threads.sleep(2000);
                }
            }).run();

            new Thread(() -> {
                while (true) {
                    events("user-1", "prod-2").forEach(store1::append);
                    events("user-2", "prod-1").forEach(store2::append);
                    Threads.sleep(1500);
                }
            }).run();

            Thread.currentThread().join();

//            linkTo(store1, "user-1", ev -> ev.type);
//            linkTo(store2, "user-2", ev -> ev.type);


//            System.out.println("------ STORE 1 ------");
//            print(store1);
//
//            System.out.println("------ STORE 2 ------");
//            print(store2);
//
//            LogIterator<EventRecord> ordered = fromStreams("EV_", store1, store2);
//            System.out.println("------ CAs ------");
//            print(ordered);
//
//            LogIterator<EventRecord> all = fromAll(store1, store2);
//            System.out.println("------ ALL ------");
//            print(all);

        }
    }

    private static CloseableIterator<EventRecord> fromStream(StreamName stream, EventStore... stores) {
        List<StreamIterator> its = Arrays.stream(stores).map(s -> s.fromStream(stream)).collect(Collectors.toList());
        return Iterators.ordered(its, er -> er.timestamp);
    }

    private static CloseableIterator<EventRecord> fromStreams(String prefix, EventStore... stores) {
        List<StreamIterator> its = Arrays.stream(stores).map(s -> s.fromStreams(prefix)).collect(Collectors.toList());
        return Iterators.ordered(its, er -> er.timestamp);
    }

    private static CloseableIterator<EventRecord> fromAll(EventStore... stores) {
        List<EventLogIterator> its = Arrays.stream(stores).map(s -> s.fromAll(LinkToPolicy.INCLUDE, SystemEventPolicy.IGNORE)).collect(Collectors.toList());
        return Iterators.ordered(its, er -> er.timestamp);
    }

    private static List<EventRecord> events(String user, String product) {
        byte[] data = ("PRODUCT => " + product).getBytes(StandardCharsets.UTF_8);
        return List.of(EventRecord.create(user, VP, data), EventRecord.create(user, CA, data), EventRecord.create(user, OC, data));
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
