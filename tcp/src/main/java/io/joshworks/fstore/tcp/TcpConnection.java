package io.joshworks.fstore.tcp;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.buffers.StupidPool;
import io.joshworks.fstore.serializer.kryo.KryoSerializer;
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

import static java.util.Objects.requireNonNull;

public class TcpConnection implements Closeable {

    public static final Logger logger = LoggerFactory.getLogger(TcpConnection.class);

    private final StreamConnection connection;
    protected final StupidPool writePool;
    private final long since = System.currentTimeMillis();
    private long bytesSent;
    private long bytesReceived;
    private long messagesSent;
    private long messagesReceived;


    public TcpConnection(StreamConnection connection, StupidPool writePool) {
        this.connection = connection;
        this.writePool = writePool;
    }

    public void send(ByteBuffer buffer) {
        requireNonNull(buffer, "Data must node be null");
        try {
            write(buffer, false);
        } catch (IOException e) {
            throw new RuntimeIOException("Failed to write entry", e);
        }
    }

    public void send(byte[] bytes) {
        writeBytes(bytes, false);
    }

    public void send(Object data) {
        writeObject(data, false);
    }

    public void sendAndFlush(ByteBuffer buffer) {
        requireNonNull(buffer, "Data must node be null");
        try {
            write(buffer, false);
        } catch (IOException e) {
            throw new RuntimeIOException("Failed to write entry", e);
        }
    }

    public void sendAndFlush(byte[] bytes) {
        writeBytes(bytes, true);
    }

    public void sendAndFlush(Object data) {
        writeObject(data, true);
    }

    private void writeObject(Object data, boolean flush) {
        requireNonNull(data, "Data must node be null");
        ByteBuffer buffer = writePool.allocate();
        try {
            KryoSerializer.serialize(data, buffer);
            buffer.flip();
            write(buffer, flush);
        } catch (IOException e) {
            throw new RuntimeIOException("Failed to write " + data, e);
        } finally {
            writePool.free(buffer);
        }
    }

    private void writeBytes(byte[] bytes, boolean flush) {
        requireNonNull(bytes, "Data must node be null");
        ByteBuffer buffer = writePool.allocate();
        try {
            buffer.put(bytes);
            buffer.flip();
            write(buffer, flush);
        } catch (IOException e) {
            throw new RuntimeIOException("Failed to write data", e);
        } finally {
            writePool.free(buffer);
        }
    }

    protected void write(ByteBuffer buffer, boolean flush) throws IOException {
        var sink = connection.getSinkChannel();
        if (!sink.isOpen()) {
            throw new IllegalStateException("Closed channel");
        }
        Channels.writeBlocking(sink, buffer);
        if (flush) {
            Channels.flushBlocking(sink);
        }
        incrementMessageSent();
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


    void updateBytesSent(long bytes) {
        this.bytesSent += bytes;
    }

    void updateBytesReceived(long bytes) {
        this.bytesReceived += bytes;
    }

    XnioWorker worker() {
        return connection.getWorker();
    }

    public long elapsed() {
        return System.currentTimeMillis() - since;
    }

    public void incrementMessageReceived() {
        messagesReceived++;
    }

    public void incrementMessageSent() {
        messagesSent++;
    }

    public long messagesSent() {
        return messagesSent;
    }

    public long messagesReceived() {
        return messagesReceived;
    }

    public long bytesReceived() {
        return bytesReceived;
    }

    public long bytesSent() {
        return bytesSent;
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
