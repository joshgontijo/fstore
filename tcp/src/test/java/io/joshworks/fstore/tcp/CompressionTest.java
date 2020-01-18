package io.joshworks.fstore.tcp;

import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.tcp.codec.Compression;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class CompressionTest {

    private static final String HOST = "localhost";
    private static final int PORT = 9999;

    private TcpEventServer server;
    private TcpConnection client;

    private static final int ITEMS = 10000;
    private CountDownLatch latch;
    private final Set<String> received = new ConcurrentSkipListSet<>();

    @Before
    public void setUp() {
        latch = new CountDownLatch(ITEMS);
        server = TcpEventServer.create()
                .compression(Compression.SNAPPY)
                .onEvent((conn, data) -> {
                    synchronized (CompressionTest.class) {
                        ByteBuffer buff = (ByteBuffer) data;
                        String val = StandardCharsets.UTF_8.decode(buff).toString();
                        received.add(val);
                        latch.countDown();
                    }
                })
                .start(new InetSocketAddress(HOST, PORT));

        client = TcpEventClient.create()
                .compression(Compression.SNAPPY)
                .connect(new InetSocketAddress(HOST, PORT), 5, TimeUnit.SECONDS);
    }

    protected abstract Compression compression();

    @After
    public void tearDown() {
        IOUtils.closeQuietly(client);
        IOUtils.closeQuietly(server);
        received.clear();
    }

    @Test
    public void sendCompressed_no_flush() throws Exception {
        sendRaw(false);
    }

    @Test
    public void sendCompressed_flush() throws Exception {
        sendRaw(true);
    }

    public void sendRaw(boolean flush) throws Exception {
        for (int i = 0; i < ITEMS; i++) {
            byte[] value = longStringOf(i).getBytes(StandardCharsets.UTF_8);
            client.send(ByteBuffer.wrap(value), flush);
        }

        if (!latch.await(30, TimeUnit.SECONDS)) {
            fail("Didnt receive all messages from the server");
        }

        assertEquals(ITEMS, received.size());

        for (int i = 0; i < ITEMS; i++) {
            String expected = longStringOf(i);
            assertTrue("Server did not receive " + i, received.contains(expected));
        }
    }

    private static String longStringOf(int i) {
        return IntStream.range(0, 100).map(a -> i).mapToObj(String::valueOf).collect(Collectors.joining("-"));
    }

    public static class NoCompression extends CompressionTest {
        @Override
        protected Compression compression() {
            return Compression.NONE;
        }
    }

    public static class SnappyCompression extends CompressionTest {
        @Override
        protected Compression compression() {
            return Compression.SNAPPY;
        }
    }

    public static class LZ4HighCompression extends CompressionTest {
        @Override
        protected Compression compression() {
            return Compression.LZ4_HIGH;
        }
    }

    public static class LZ4FastCompression extends CompressionTest {
        @Override
        protected Compression compression() {
            return Compression.LZ4_FAST;
        }
    }

    public static class DeflateCompression extends CompressionTest {
        @Override
        protected Compression compression() {
            return Compression.DEFLATE;
        }
    }

}
