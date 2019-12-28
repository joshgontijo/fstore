package io.joshworks.fstore.cluster;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.util.Threads;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ClusterNodeTest {

    private String cluster = "test-cluster";
    private String node1Id = "node-1";
    private String node2Id = "node-2";

    private ClusterNode clusterNode1;
    private ClusterNode clusterNode2;

    @Before
    public void setUp() {
        System.setProperty("java.net.preferIPv4Stack", "true");
        System.setProperty("jgroups.bind_addr", "127.0.0.1");

        clusterNode1 = new ClusterNode(cluster, node1Id);
        clusterNode2 = new ClusterNode(cluster, node2Id);
        clusterNode1.interceptor(new LoggingInterceptor());
        clusterNode1.join();
        clusterNode2.join();
    }

    @After
    public void tearDown() {
        IOUtils.closeQuietly(clusterNode1);
        IOUtils.closeQuietly(clusterNode2);
    }

    @Test
    public void send_returns_correct_payload() {

        final var pong = new PongMessage();
        clusterNode1.register(PingMessage.class, (addr, ping) -> pong);
        clusterNode2.register(PongMessage.class, (addr, ping) -> {
        });

        Object resp = clusterNode2.client().send(clusterNode1.address(), new PingMessage());
        assertEquals(pong, resp);
    }

    @Test
    public void sendAsync_correct_payload() throws InterruptedException {

        PingMessage ping = new PingMessage();
        AtomicReference<PingMessage> captured = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        clusterNode1.register(PingMessage.class, (addr, p) -> {
            captured.set(p);
            latch.countDown();
            return null;
        });

        clusterNode2.client().sendAsync(clusterNode1.address(), ping);
        latch.await(10, TimeUnit.SECONDS);
        assertEquals(ping, captured.get());
    }

    @Test
    public void cast_returns_correct_payload() {

        final var pong = new PongMessage();
        clusterNode1.register(PingMessage.class, (addr, p) -> pong);
        clusterNode2.register(PongMessage.class, (addr, p) -> {
        });

        List<MulticastResponse> responses = clusterNode2.client().cast(new PingMessage());
        assertEquals(1, responses.size());
        assertEquals(pong, responses.get(0).message());
    }

    @Test
    public void castAsync_returns_correct_payload() throws InterruptedException {

        PingMessage ping = new PingMessage();
        AtomicReference<PingMessage> captured = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        clusterNode1.register(PingMessage.class, (addr, p) -> {
            captured.set(p);
            latch.countDown();
            return null;
        });

        clusterNode2.client().castAsync(ping);
        latch.await(10, TimeUnit.SECONDS);
        assertEquals(ping, captured.get());
    }

    @Test
    public void send_returns_null() {

        PingMessage ping = new PingMessage();
        clusterNode1.register(PingMessage.class, (addr, p) -> null);

        Object result = clusterNode2.client().send(clusterNode1.address(), ping);
        assertNull(result);
    }

    @Test
    public void cast_returns_null() {

        PingMessage ping = new PingMessage();
        clusterNode1.register(PingMessage.class, p -> null);

        List<MulticastResponse> results = clusterNode2.client().cast(ping);
        assertEquals(1, results.size());
        assertNull(results.get(0).message());
    }


    @Test
    public void executor() {

        var task = (Callable<String> & Serializable) () -> {
            System.out.println("STARTING TASK");
            Threads.sleep(1000);
            System.out.println("COMPLETED TASK");
            return UUID.randomUUID().toString();
        };

        IntStream.range(0, 20).boxed().map(i -> clusterNode2.client().executor().submit(task))
                .map(v -> "RESULT: " + Threads.futureGet(v)).forEach(System.out::println);

        Threads.sleep(60000);
    }


}