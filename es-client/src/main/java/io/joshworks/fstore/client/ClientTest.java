package io.joshworks.fstore.client;

import io.joshworks.fstore.es.shared.EventRecord;
import io.joshworks.fstore.es.shared.routing.HashRouter;

import java.net.InetSocketAddress;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ClientTest {

    private static final String STREAM_PREFIX = "stream-";
    private static final int STREAMS = 10000000;
    private static final int ITEMS = 10000000;
    private static final int LOG_INTERVAL = 10000;

    public static void main(String[] args) throws Exception {

        StoreClient storeClient = StoreClient.connect(new HashRouter(), new InetSocketAddress("localhost", 10000));

        long start = System.currentTimeMillis();
        long s = System.currentTimeMillis();
        for (int i = 0; i < ITEMS; i++) {
            String stream = STREAM_PREFIX + (i % STREAMS);
            storeClient.append(stream, "USER_CREATED", new UserCreated("josh", i));
            if (i % LOG_INTERVAL == 0) {
                System.out.println("WRITE: " + i + " IN " + (System.currentTimeMillis() - start));
                start = System.currentTimeMillis();
            }
        }
        System.out.println("TOTAL WRITES: " + ITEMS + " IN " + (System.currentTimeMillis() - s));


        System.out.println("----------------- READ ---------------");
        NodeClientIterator iterator = storeClient.iterator(50, "stream-2", "stream-4");
        iterateAll(iterator);

        System.out.println("----------------- READ ALL ---------------");
        iterator = storeClient.iterator(50, "stream-*");
        iterateAll(iterator);


        storeClient.close();

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
