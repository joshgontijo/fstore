package io.joshworks.fstore.cluster;

import io.joshworks.fstore.core.util.Threads;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class JGroupsTest {

    private static final AtomicLong counter = new AtomicLong();

    public static void main(String[] args) {

        System.setProperty("java.net.preferIPv4Stack", "true");
        System.setProperty("jgroups.bind_addr", "127.0.0.1");

        Cluster cluster1 = new Cluster("test", "1");
        Cluster cluster2 = new Cluster("test", "2");

        cluster1.register(Message.class, (addr, msg) -> {
            counter.incrementAndGet();
        });

        cluster1.join();
        cluster2.join();

        int items = 1_000_000;
        long s1 = System.currentTimeMillis();
        long start = System.currentTimeMillis();
        for (int i = 0; i < items; i++) {
            cluster2.client().sendAsync(cluster1.address(), new Message(123, UUID.randomUUID().toString()));
        }

        while (counter.get() < items) {
            long now = System.currentTimeMillis();
            System.out.println(counter.get() + " -> " + (now - start));
            Threads.sleep(1000);
        }

        System.out.println(counter.get());
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
