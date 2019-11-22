package io.joshworks.fstore.tcp;

import io.joshworks.fstore.core.io.buffers.BufferPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xnio.IoUtils;
import org.xnio.StreamConnection;
import org.xnio.XnioWorker;
import org.xnio.channels.Channels;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

public class TcpConnection implements Closeable {

    public static final Logger logger = LoggerFactory.getLogger(TcpConnection.class);

    private final StreamConnection connection;
    protected final BufferPool writePool;
    private final long since = System.currentTimeMillis();
    private final AtomicLong bytesSent = new AtomicLong(); //TODO long fieldupdater
    private final AtomicLong bytesReceived = new AtomicLong(); //TODO long fieldupdater
    private final AtomicLong messagesSent = new AtomicLong(); //TODO long fieldupdater
    private final AtomicLong messagesReceived = new AtomicLong(); //TODO long fieldupdater


    public TcpConnection(StreamConnection connection, BufferPool writePool) {
        this.connection = connection;
        this.writePool = writePool;
    }

    public void send(Object data) {
        if (data == null) {
            return;
        }
        try (writePool) {
            ByteBuffer buffer = writePool.allocate();
            LengthPrefixCodec.serialize(data, buffer);
            buffer.flip();
            write(buffer, false);
        }
    }

    public void sendAndFlush(Object data) {
        if (data == null) {
            return;
        }
        try (writePool) {
            ByteBuffer buffer = writePool.allocate();
            LengthPrefixCodec.serialize(data, buffer);
            buffer.flip();
            if (!buffer.hasRemaining()) {
                throw new RuntimeException("Empty buffer");
            }
            write(buffer, true);
        }
    }

    public long elapsed() {
        return System.currentTimeMillis() - since;
    }

    protected void write(ByteBuffer buffer, boolean flush) {
        var sink = connection.getSinkChannel();
        if (!sink.isOpen()) {
            throw new IllegalStateException("Closed channel");
        }
        try {
            Channels.writeBlocking(sink, buffer);
            if (flush) {
                Channels.flushBlocking(sink);
            }
            incrementMessageSent();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            Channels.flushBlocking(connection.getSinkChannel());
        } catch (Exception e) {
            //TODO log ?
            System.err.println("Failed to flush buffer when closing");
        }
        IoUtils.safeClose(connection);
        connection.getWorker().shutdown();
    }

    public InetSocketAddress peerAddress() {
        return connection.getPeerAddress(InetSocketAddress.class);
    }

    public long bytesReceived() {
        return bytesReceived.get();
    }

    public long bytesSent() {
        return bytesSent.get();
    }

    void updateBytesSent(long bytes) {
        this.bytesSent.addAndGet(bytes);
    }

    void updateBytesReceived(long bytes) {
        this.bytesReceived.addAndGet(bytes);
    }

    XnioWorker worker() {
        return connection.getWorker();
    }

    public void incrementMessageReceived() {
        messagesReceived.incrementAndGet();
    }

    public void incrementMessageSent() {
        messagesSent.incrementAndGet();
    }

    public long messagesSent() {
        return messagesSent.get();
    }

    public long messagesReceived() {
        return messagesReceived.get();
    }

    @Override
    public String toString() {
        return "TcpConnection{" + "since=" + since +
                ", peerAddress=" + peerAddress() +
                ", bytesSent=" + bytesSent +
                ", bytesReceived=" + bytesReceived +
                ", messagesSent=" + messagesSent +
                ", messagesReceived=" + messagesReceived +
                '}';
    }

}
