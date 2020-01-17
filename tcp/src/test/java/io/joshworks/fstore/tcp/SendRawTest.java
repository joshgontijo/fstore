package io.joshworks.fstore.tcp;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.tcp.TcpConnection;
import io.joshworks.fstore.tcp.TcpEventClient;
import io.joshworks.fstore.tcp.TcpEventServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SendRawTest {

    private static final String HOST = "localhost";
    private static final int PORT = 9999;

    private TcpEventServer server;
    private TcpConnection client;

    private static final String MESSAGE = "Hello event!";
    private CountDownLatch latch = new CountDownLatch(1);
    private AtomicReference<String> received = new AtomicReference<>();

    @Before
    public void setUp() {
        server = TcpEventServer.create()
                .onEvent((conn, data) -> {
                    ByteBuffer buff = (ByteBuffer) data;
                    CharBuffer charBuff = StandardCharsets.UTF_8.decode(buff);
                    received.set(charBuff.toString());
                    latch.countDown();
                })
                .start(new InetSocketAddress(HOST, PORT));

        client = TcpEventClient.create().connect(new InetSocketAddress(HOST, PORT), 5, TimeUnit.SECONDS);

    }

    @After
    public void tearDown() {
        IOUtils.closeQuietly(client);
        IOUtils.closeQuietly(server);
    }

    @Test
    public void sendRaw() throws InterruptedException {
        client.send(ByteBuffer.wrap(MESSAGE.getBytes(StandardCharsets.UTF_8)));

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
