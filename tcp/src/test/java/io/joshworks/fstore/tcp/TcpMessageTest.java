package io.joshworks.fstore.tcp;

import io.joshworks.fstore.core.io.buffers.Buffers;
import org.junit.Test;

import static io.joshworks.fstore.tcp.TcpMessage.BYTES;
import static io.joshworks.fstore.tcp.TcpMessage.compression;
import static io.joshworks.fstore.tcp.TcpMessage.internal;
import static org.junit.Assert.assertEquals;

public class TcpMessageTest {

    @Test
    public void internal_test() {
        var b = Buffers.allocate(BYTES, false);
        internal(b, TcpMessage.Internal.ACK);
        compression(b, TcpMessage.Compression.DEFLATE);

        assertEquals(TcpMessage.Internal.ACK, internal(b));
        assertEquals(TcpMessage.Compression.DEFLATE, compression(b));
    }
}