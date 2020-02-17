package io.joshworks.fstore.tcp;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.serializer.kryo.KryoSerializer;
import io.joshworks.fstore.tcp.codec.CodecRegistry;
import io.joshworks.fstore.tcp.codec.Compression;
import io.joshworks.fstore.tcp.codec.TcpHeader;
import io.joshworks.fstore.tcp.internal.Message;
import io.joshworks.fstore.tcp.internal.Response;
import io.joshworks.fstore.tcp.internal.ResponseTable;
import io.joshworks.fstore.tcp.internal.RpcEvent;
import org.xnio.IoUtils;
import org.xnio.StreamConnection;
import org.xnio.XnioWorker;
import org.xnio.channels.Channels;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static java.util.Objects.requireNonNull;

public class TcpConnection implements Closeable {

    private static final AtomicLongFieldUpdater<TcpConnection> bytesSentUpdater = AtomicLongFieldUpdater.newUpdater(TcpConnection.class, "bytesSent");
    private static final AtomicLongFieldUpdater<TcpConnection> bytesReceivedUpdater = AtomicLongFieldUpdater.newUpdater(TcpConnection.class, "bytesReceived");
    private static final AtomicLongFieldUpdater<TcpConnection> messagesSentUpdater = AtomicLongFieldUpdater.newUpdater(TcpConnection.class, "messagesSent");
    private static final AtomicLongFieldUpdater<TcpConnection> messagesReceivedUpdater = AtomicLongFieldUpdater.newUpdater(TcpConnection.class, "messagesReceived");

    private final StreamConnection connection;
    final ResponseTable responseTable;
    private final AtomicLong reqids = new AtomicLong();
    private final long since = System.currentTimeMillis();
    private volatile long bytesSent;
    private volatile long bytesReceived;
    private volatile long messagesSent;
    private volatile long messagesReceived;
    private final BufferPool pool;
    private final Compression compression;

    public TcpConnection(StreamConnection connection, BufferPool pool, ResponseTable responseTable, Compression compression) {
        this.connection = connection;
        this.responseTable = responseTable;
        this.pool = pool;
        this.compression = compression;
    }

    //-------------- REQUEST - RESPONSE
    public <R> Response<R> request(Object data) {
        requireNonNull(data, "Entity must be provided");
        long reqId = reqids.getAndIncrement();
        Message message = new Message(reqId, data);
        Response<R> response = responseTable.newRequest(reqId);
        write(message, false);
        return response;
    }

    //--------------------- RPC ------------

    /**
     * Expects a return from the server, calling void methods will return null
     */
    public <R> Response<R> invoke(String method, Object... params) {
        RpcEvent event = new RpcEvent(method, params);
        return request(event);
    }

    /**
     * Fire and forget, response from the server is ignored
     */
    public void invokeAsync(String method, Object... params) {
        RpcEvent event = new RpcEvent(method, params);
        write(event, false);
    }

    /**
     * Creates a proxy instance that delegates calls to the remote node
     *
     * @param timeoutMillis request timeout, less than zero for no timeout
     */
    public <T> T createRpcProxy(Class<T> type, int timeoutMillis) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(),
                new Class[]{type},
                new RpcProxyHandler(timeoutMillis));
    }

    //---------------------------------

    public void send(ByteBuffer buffer, boolean flush) {
        requireNonNull(buffer, "Data must node be null");
        doWrite(buffer, flush);
    }

    public void send(byte[] bytes, boolean flush) {
        write(bytes, flush);
    }

    public void send(Object data, boolean flush) {
        write(data, flush);
    }

    void write(Object data, boolean flush) {
        requireNonNull(data, "Data must node be null");
        ByteBuffer buffer = pool.allocate();
        try {
            KryoSerializer.serialize(data, buffer);
            buffer.flip();
            doWrite(buffer, flush);
        } finally {
            pool.free(buffer);
        }
    }

    private void write(byte[] bytes, boolean flush) {
        requireNonNull(bytes, "Data must node be null");
        ByteBuffer buffer = pool.allocate();
        try {
            buffer.put(bytes);
            buffer.flip();
            doWrite(buffer, flush);
        } finally {
            pool.free(buffer);
        }

    }

    private synchronized void doWrite(ByteBuffer src, boolean flush) {
        requireNonNull(src, "Data must node be null");
        ByteBuffer buffer = pool.allocate();
        try {
            Buffers.offsetPosition(buffer, TcpHeader.BYTES);
            int uncompressedLen = src.remaining();
            if (Compression.NONE.equals(compression)) {
                int copied = Buffers.copy(src, buffer);
                Buffers.offsetPosition(src, copied);
            } else {
                CodecRegistry.lookup(compression).compress(src, buffer);
            }

            buffer.flip();
            TcpHeader.uncompressedLength(buffer, uncompressedLen);
            TcpHeader.compression(buffer, compression);

            var sink = connection.getSinkChannel();
            if (!sink.isOpen()) {
                throw new IllegalStateException("Closed channel");
            }

            Channels.writeBlocking(sink, buffer);
            if (flush) {
                Channels.flushBlocking(sink);
            }
            incrementMessageSent();

        } catch (IOException e) {
            throw new RuntimeIOException("Failed to write data", e);
        } finally {
            pool.free(buffer);
        }
    }

    public void flush() throws IOException {
        Channels.flushBlocking(connection.getSinkChannel());
    }

    @Override
    public void close() {
        IoUtils.safeClose(connection);
        connection.getWorker().shutdown();
    }

    public InetSocketAddress peerAddress() {
        return connection.getPeerAddress(InetSocketAddress.class);
    }

    void updateBytesSent(long bytes) {
        bytesSentUpdater.addAndGet(this, bytes);
    }

    void updateBytesReceived(long bytes) {
        bytesReceivedUpdater.addAndGet(this, bytes);
    }

    XnioWorker worker() {
        return connection.getWorker();
    }

    public long elapsed() {
        return System.currentTimeMillis() - since;
    }

    public void incrementMessageReceived() {
        messagesReceivedUpdater.incrementAndGet(this);
    }

    public void incrementMessageSent() {
        messagesSentUpdater.incrementAndGet(this);
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

    public BufferPool pool() {
        return pool;
    }


    private class RpcProxyHandler implements InvocationHandler {

        private final int timeoutMillis;

        private RpcProxyHandler(int timeoutMillis) {
            this.timeoutMillis = timeoutMillis;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            String methodName = method.getName();
            Response<Object> invocation = TcpConnection.this.invoke(methodName, args);
            if (method.getReturnType().isAssignableFrom(Future.class)) {
                return invocation;
            }
            if (timeoutMillis < 0) {
                return invocation.get();
            }
            return invocation.get(timeoutMillis, TimeUnit.MILLISECONDS);
        }
    }


}
