package io.joshworks.eventry.network;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.util.Threads;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;

public class ClusterTest {

    private String cluster = "test-cluster";
    private String node1Id = "node-1";
    private String node2Id = "node-2";

    private Cluster node1;
    private Cluster node2;

    @Before
    public void setUp() {
        System.setProperty("java.net.preferIPv4Stack", "true");
        System.setProperty("jgroups.bind_addr", "127.0.0.1");

        node1 = new Cluster(cluster, node1Id);
        node2 = new Cluster(cluster, node2Id);
        node1.interceptor(new LoggingInterceptor());
        node2.interceptor(new LoggingInterceptor());
        node1.join();
        node2.join();
    }

    @After
    public void tearDown() {
        IOUtils.closeQuietly(node1);
        IOUtils.closeQuietly(node2);
    }

    @Test
    public void send_returns_correct_payload() {

        final var pong = new PongMessage();
        node1.register(PingMessage.class, ping -> pong);
        node2.register(PongMessage.class, png -> {});

        ClusterMessage resp = node2.client().send(node1.address(), new PingMessage());
        assertEquals(pong, resp);
    }

    @Test
    public void sendAsync_correct_payload() throws InterruptedException {

        PingMessage ping = new PingMessage();
        AtomicReference<PingMessage> captured = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        node1.register(PingMessage.class, p -> {
            captured.set(p);
            latch.countDown();
            return null;
        });

        node2.client().sendAsync(node1.address(), ping);
        latch.await(10, TimeUnit.SECONDS);
        assertEquals(ping, captured.get());
    }

    @Test
    public void cast_returns_correct_payload() {

        final var pong = new PongMessage();
        node1.register(PingMessage.class, ping -> pong);
        node2.register(PongMessage.class, png -> {});

        List<MulticastResponse> responses = node2.client().cast(new PingMessage());
        assertEquals(1, responses.size());
        assertEquals(pong, responses.get(0).message());
    }

    @Test
    public void castAsync_returns_correct_payload() throws InterruptedException {

        PingMessage ping = new PingMessage();
        AtomicReference<PingMessage> captured = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        node1.register(PingMessage.class, p -> {
            captured.set(p);
            latch.countDown();
            return null;
        });

        node2.client().castAsync(ping);
        latch.await(10, TimeUnit.SECONDS);
        assertEquals(ping, captured.get());
    }

}