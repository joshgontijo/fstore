package io.joshworks.fstore.tcp;

import io.joshworks.fstore.core.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ServerPushTest {

    private static final String HOST = "localhost";
    private static final int PORT = 9999;
    private static final String MESSAGE = "Hello push event!";
    private TcpEventServer server;
    private TcpConnection client;
    private CountDownLatch latch = new CountDownLatch(1);
    private AtomicReference<String> received = new AtomicReference<>();

    @Before
    public void setUp() {
        server = TcpEventServer.create()
                .start(new InetSocketAddress(HOST, PORT));
        client = TcpEventClient.create()
                .onEvent(TypedEventHandler.builder().on(PushEvent.class, (conn, event) -> {
                    received.set(event.message);
                    latch.countDown();

                }).build())
                .connect(new InetSocketAddress(HOST, PORT), 5, TimeUnit.SECONDS);

    }

    @After
    public void tearDown() {
        IOUtils.closeQuietly(client);
        IOUtils.closeQuietly(server);
    }

    @Test
    public void push() throws InterruptedException {
        server.broadcast(new PushEvent(MESSAGE));

        if (!latch.await(5, TimeUnit.SECONDS)) {
            fail("Didnt receive message from the server");
        }
        assertEquals(MESSAGE, received.get());
    }

    private static class PushEvent {
        public final String message;

        private PushEvent(String message) {
            this.message = message;
        }
    }


}
