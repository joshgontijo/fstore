package io.joshworks.fstore.tcp;

import io.joshworks.fstore.tcp.codec.Compression;
import org.xnio.channels.Channels;
import org.xnio.conduits.ConduitStreamSinkChannel;

import java.nio.ByteBuffer;

import static io.joshworks.fstore.tcp.TcpConnection.write0;
import static java.util.Objects.requireNonNull;

public class BatchWriter {

    private final ByteBuffer backingBuffer;
    private final Compression compression;
    private final ConduitStreamSinkChannel sink;

    BatchWriter(ByteBuffer backingBuffer, Compression compression, ConduitStreamSinkChannel sink) {
        this.backingBuffer = backingBuffer;
        this.compression = compression;
        this.sink = sink;
    }

    public void write(ByteBuffer src) {
        try {
            requireNonNull(src, "Data must node be null");
            if (backingBuffer.remaining() < TcpHeader.BYTES + src.remaining()) {
                flush(false);
            }
            write0(src, backingBuffer, compression);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void flush(boolean force) {
        try {
            backingBuffer.flip();
            if (backingBuffer.remaining() <= TcpHeader.BYTES) {
                if (force) {
                    Channels.flushBlocking(sink);
                }
                backingBuffer.clear();
                return;
            }
            Channels.writeBlocking(sink, backingBuffer);
            if (force) {
                Channels.flushBlocking(sink);
            }
            backingBuffer.clear();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
