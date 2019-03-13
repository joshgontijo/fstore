package io.joshworks.eventry.network;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.util.Threads;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ClusterTest {

    private String cluster = "test-cluster";
    private String node1Id = "node-1";
    private String node2Id = "node-2";

    private Cluster node1;
    private Cluster node2;

    @Before
    public void setUp() {
        node1 = new Cluster(cluster, node1Id);
        node2 = new Cluster(cluster, node2Id);
        node1.interceptor(new LoggingInterceptor());
        node2.interceptor(new LoggingInterceptor());
    }

    @After
    public void tearDown() {
        IOUtils.closeQuietly(node1);
        IOUtils.closeQuietly(node2);
    }

    @Test
    public void receive_message() {

        final var pong = new PongMessage();
        node1.register(PingMessage.class, ping -> pong);
        node2.register(PongMessage.class, png -> {
            System.out.println("");
            return null;
        });

        node1.join();
        node2.join();

        node2.client().sendAsync(node1.address(), new PingMessage());
//        assertEquals(pong, resp);
        Threads.sleep(500);

    }
}