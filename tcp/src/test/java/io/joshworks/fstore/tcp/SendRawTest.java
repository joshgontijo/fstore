package io.joshworks.fstore.tcp;

import io.joshworks.fstore.core.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SendRawTest {

    private static final String HOST = "localhost";
    private static final int PORT = 9999;

    private TcpEventServer server;
    private TcpConnection client;

    private static final int ITEMS = 10000;
    private CountDownLatch latch;
    private final Set<Integer> received = new ConcurrentSkipListSet<>();

    @Before
    public void setUp() {
        latch = new CountDownLatch(ITEMS);
        server = TcpEventServer.create()
                .onEvent((conn, data) -> {
                    synchronized (SendRawTest.class) {
                        ByteBuffer buff = (ByteBuffer) data;
                        received.add(buff.getInt());
                        latch.countDown();
                    }
                })
                .start(new InetSocketAddress(HOST, PORT));

        client = TcpEventClient.create().connect(new InetSocketAddress(HOST, PORT), 5, TimeUnit.SECONDS);

    }

    @After
    public void tearDown() {
        IOUtils.closeQuietly(client);
        IOUtils.closeQuietly(server);
        received.clear();
    }

    @Test
    public void sendRaw_no_flush() throws Exception {
        sendRaw(false);
    }

    @Test
    public void sendRaw_flush() throws Exception {
        sendRaw(true);
    }

    public void sendRaw(boolean flush) throws Exception {
        for (int i = 0; i < ITEMS; i++) {
            client.send(ByteBuffer.allocate(Integer.BYTES).putInt(i).flip(), flush);
        }

        if (!latch.await(30, TimeUnit.SECONDS)) {
            fail("Didnt receive all messages from the server");
        }

        assertEquals(ITEMS, received.size());

        for (int i = 0; i < ITEMS; i++) {
            assertTrue("Server did not receive " + i, received.contains(i));
        }
    }

}
