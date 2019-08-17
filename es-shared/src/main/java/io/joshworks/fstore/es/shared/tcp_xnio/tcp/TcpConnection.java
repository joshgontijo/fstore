package io.joshworks.fstore.es.shared.tcp_xnio.tcp;

import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.io.buffers.SimpleBufferPool;
import org.xnio.IoUtils;
import org.xnio.StreamConnection;
import org.xnio.XnioWorker;
import org.xnio.channels.Channels;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

public class TcpConnection implements Closeable {

    private final StreamConnection connection;
    private final SimpleBufferPool bufferPool;
    private final long since = System.currentTimeMillis();
    private long bytesSent; //TODO long fieldupdater
    private long bytesReceived; //TODO long fieldupdater
    private long messagesSent; //TODO long fieldupdater
    private long messagesReceived; //TODO long fieldupdater
    private long lastActivity;

    TcpConnection(StreamConnection connection, SimpleBufferPool bufferPool) {
        this.connection = connection;
        this.bufferPool = bufferPool;
    }

    public <T> void send(T data) {
        if (data == null) {
            return;
        }
        try(SimpleBufferPool.BufferRef bufferRef = bufferPool.allocateRef()) {
            ByteBuffer buffer = bufferRef.buffer;
            LengthPrefixCodec.write(data, buffer);
            buffer.flip();
            write(buffer, false);
        }
    }

    public <T> void sendAndFlush(T data) {
        if (data == null) {
            return;
        }
        try(SimpleBufferPool.BufferRef bufferRef = bufferPool.allocateRef()) {
            ByteBuffer buffer = bufferRef.buffer;
            LengthPrefixCodec.write(data, buffer);
            buffer.flip();
            write(buffer, true);
        }
    }

    public long elapsed() {
        return System.currentTimeMillis() - since;
    }

    private void write(ByteBuffer data, boolean flush) {
        var sink = connection.getSinkChannel();
        if (!sink.isOpen()) {
            throw new IllegalStateException("Closed channel");
        }
        try {
            //TODO this might case buffer problems when 'data' is from a bufferpool in which clears the buffers
            // before fully flushed to the wire
            sink.write(data);
            if (flush) {
                Channels.flushBlocking(sink);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            Channels.flushBlocking(connection.getSinkChannel());
        } catch (Exception e) {
            System.err.println("Failed to flush buffer when closing");
        }
        IoUtils.safeClose(connection);
        connection.getWorker().shutdown();
    }

    public InetSocketAddress peerAddress() {
        return connection.getPeerAddress(InetSocketAddress.class);
    }

    public long bytesReceived() {
        return bytesReceived;
    }

    public long bytesSent() {
        return bytesSent;
    }

    public long lastIdleTimeoutAck() {
        return lastActivity;
    }

    void updateBytesSent(long bytes) {
        this.bytesSent += bytes;
    }

    void updateBytesReceived(long bytes) {
        this.bytesReceived += bytes;
    }

    void idleTimeoutAck() {
        this.lastActivity = System.currentTimeMillis();
    }

    XnioWorker worker() {
        return connection.getWorker();
    }

    BufferPool bufferPool() {
        return bufferPool;
    }

}
