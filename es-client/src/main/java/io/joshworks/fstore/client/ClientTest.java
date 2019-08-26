package io.joshworks.fstore.client;

import io.joshworks.fstore.es.shared.EventRecord;

import java.net.MalformedURLException;
import java.net.URL;

public class ClientTest {
    public static void main(String[] args) throws MalformedURLException, InterruptedException {

        StoreClient storeClient = StoreClient.connect(new URL("http://localhost:9000"));
        String stream1 = "stream-1";
        String stream2 = "stream-2";

        storeClient.createStream(stream1);
        storeClient.createStream(stream2);


        Thread t1 = new Thread(() -> {
            try {
                long start = System.currentTimeMillis();
                for (int i = 0; i < 5000000; i++) {
//            EventCreated eventCreated = storeClient.append(stream, "USER_CREATED", new UserCreated("josh", 123));
                    storeClient.appendAsync(stream1, "USER_CREATED", new UserCreated("josh", 123));
                    if (i % 10000 == 0) {
                        System.out.println("WRITE: " + i + " IN " + (System.currentTimeMillis() - start));
                        start = System.currentTimeMillis();
                    }
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        });

//        Thread t2 = new Thread(() -> {
//            long start = System.currentTimeMillis();
//            for (int i = 0; i < 5000000; i++) {
////            EventCreated eventCreated = storeClient.append(stream, "USER_CREATED", new UserCreated("josh", 123));
//                storeClient.append(stream2, "USER_CREATED", new UserCreated("josh", 123));
//                if (i % 10000 == 0) {
//                    System.out.println(i + " IN " + (System.currentTimeMillis() - start));
//                    start = System.currentTimeMillis();
//                }
//            }
//        });

        t1.start();
//        t2.start();
        t1.join();
//        t2.join();

        System.out.println("----------------- READ ---------------");

        long start = System.currentTimeMillis();
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
