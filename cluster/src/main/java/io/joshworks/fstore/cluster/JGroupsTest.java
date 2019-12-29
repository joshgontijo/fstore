package io.joshworks.fstore.cluster;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class JGroupsTest {

    private static final AtomicLong counter = new AtomicLong();

    public static void main(String[] args) {

        Cluster cluster1 = new Cluster("test", "1");
        Cluster cluster2 = new Cluster("test", "2");

        cluster1.register(Message.class, (addr, msg) -> {
            counter.incrementAndGet();
        });

        cluster1.join();
        cluster2.join();

        long s1 = System.currentTimeMillis();
        long start = System.currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {
            cluster2.client().send(cluster1.address(), new Message(123, UUID.randomUUID().toString()));
            if (i % 10000 == 0) {
                long now = System.currentTimeMillis();
                System.out.println(i + " -> " + (now - start));
                start = now;
            }
        }

        System.out.println("TOOK: " + (System.currentTimeMillis() - s1));
        cluster1.close();
        cluster2.close();
    }

    public static class Message {
        private final int id;
        private final String message;

        public Message(int id, String message) {
            this.id = id;
            this.message = message;
        }
    }

}
