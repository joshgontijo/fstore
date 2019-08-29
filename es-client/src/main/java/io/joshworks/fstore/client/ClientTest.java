package io.joshworks.fstore.client;

import io.joshworks.fstore.es.shared.routing.HashRouter;
import io.joshworks.fstore.es.shared.EventRecord;

import java.net.InetSocketAddress;

public class ClientTest {
    public static void main(String[] args) throws Exception {

        StoreClient storeClient = StoreClient.connect(new HashRouter(), new InetSocketAddress("localhost", 11111));

        long start = System.currentTimeMillis();
        for (int i = 0; i < 5000000; i++) {
            String stream = "stream-" + (i % 10000);
            storeClient.appendAsync(stream, "USER_CREATED", new UserCreated("josh", 123));
            if (i % 10000 == 0) {
                System.out.println("WRITE: " + i + " IN " + (System.currentTimeMillis() - start));
                start = System.currentTimeMillis();
            }
        }

        System.out.println("----------------- READ ---------------");

        start = System.currentTimeMillis();
        int i = 0;
        NodeClientIterator iterator = storeClient.iterator("stream-*", 50);
        while (iterator.hasNext()) {
            EventRecord next = iterator.next();
//            System.out.println(next);
            if (i++ % 10000 == 0) {
                System.out.println("READ: " + i + " IN " + (System.currentTimeMillis() - start));
                start = System.currentTimeMillis();
            }
        }
        System.out.println("READ: " + i);
        iterator.close();

        System.out.println("----------------- READ AGAIN ---------------");
        iterator = storeClient.iterator("stream-*", 50);
        while (iterator.hasNext()) {
            EventRecord next = iterator.next();

        }


//        long start = System.currentTimeMillis();
//        for (int i = 0; i < 1000000; i++) {
//            EventCreated eventCreated = storeClient.appendHttp(stream, "USER_CREATED", new UserCreated("josh", 123));
//            if(i % 10000 == 0) {
//                System.out.println(i + " IN " + (System.currentTimeMillis() - start));
//                start = System.currentTimeMillis();
//            }
//        }


//
//        EventCreated eventCreated = storeClient.append(stream, "USER_CREATED", new UserCreated("josh", 123));
//        System.out.println("Added event: " + eventCreated);
//
//        EventRecord event = storeClient.get(stream, 0);
//        System.out.println(event.as(UserCreated.class));

    }
}
