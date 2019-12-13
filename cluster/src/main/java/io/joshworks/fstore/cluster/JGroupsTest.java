package io.joshworks.fstore.cluster;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class JGroupsTest {

    private static final AtomicLong counter = new AtomicLong();

    public static void main(String[] args) {

        Cluster node1 = new Cluster("test", "1");
        Cluster node2 = new Cluster("test", "2");

        node1.register(Message.class, msg -> {
            counter.incrementAndGet();
        });

        node1.join();
        node2.join();

        long s1 = System.currentTimeMillis();
        long start = System.currentTimeMillis();
        for (int i = 0; i < 1000000; i++) {
            node2.client().send(node1.address(), new Message(123, UUID.randomUUID().toString()));
            if (i % 10000 == 0) {
                long now = System.currentTimeMillis();
                System.out.println(i + " -> " + (now - start));
                start = now;
            }
        }

        System.out.println("TOOK: " + (System.currentTimeMillis() - s1));
        node1.close();
        node2.close();
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
