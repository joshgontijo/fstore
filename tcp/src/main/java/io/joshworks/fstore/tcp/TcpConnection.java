package io.joshworks.fstore.tcp;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.serializer.kryo.KryoSerializer;
import io.joshworks.fstore.tcp.internal.Message;
import io.joshworks.fstore.tcp.internal.Response;
import io.joshworks.fstore.tcp.internal.ResponseTable;
import io.joshworks.fstore.tcp.internal.RpcEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xnio.IoUtils;
import org.xnio.Pool;
import org.xnio.Pooled;
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

    public static final Logger log = LoggerFactory.getLogger(TcpConnection.class);

    private static final AtomicLongFieldUpdater<TcpConnection> bytesSentUpdater = AtomicLongFieldUpdater.newUpdater(TcpConnection.class, "bytesSent");
    private static final AtomicLongFieldUpdater<TcpConnection> bytesReceivedUpdater = AtomicLongFieldUpdater.newUpdater(TcpConnection.class, "bytesReceived");
    private static final AtomicLongFieldUpdater<TcpConnection> messagesSentUpdater = AtomicLongFieldUpdater.newUpdater(TcpConnection.class, "messagesSent");
    private static final AtomicLongFieldUpdater<TcpConnection> messagesReceivedUpdater = AtomicLongFieldUpdater.newUpdater(TcpConnection.class, "messagesReceived");

    private final StreamConnection connection;
    private final ResponseTable responseTable;
    private final AtomicLong reqids = new AtomicLong();
    private final long since = System.currentTimeMillis();
    private volatile long bytesSent;
    private volatile long bytesReceived;
    private volatile long messagesSent;
    private volatile long messagesReceived;
    private final Pooled<ByteBuffer> pooled;

    public TcpConnection(StreamConnection connection, Pool<ByteBuffer> writePool, ResponseTable responseTable) {
        this.connection = connection;
        this.responseTable = responseTable;
        pooled = writePool.allocate();
    }

    public <T, R> Response<R> request(T data) {
        requireNonNull(data, "Entity must be provided");
        long reqId = reqids.getAndIncrement();
        Message message = new Message(reqId, data);
        Response<R> response = responseTable.newRequest(reqId);
        send(message);
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
        send(event);
    }

    /**
     * Creates a proxy instance that delegates calls to the remote node
     *
     * @param timeoutMillis request timeout, less than zero for no timeout
     */
    public <T> T createRpcProxy(Class<T> type, int timeoutMillis, boolean invokeVoidAsync) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(),
                new Class[]{type},
                new RpcProxyHandler(timeoutMillis, invokeVoidAsync));
    }

    //---------------------------------

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
        synchronized (this) {
            ByteBuffer buffer = pooled.getResource().clear();
            try {
                KryoSerializer.serialize(data, buffer);
                buffer.flip();
                write(buffer, flush);
            } catch (IOException e) {
                throw new RuntimeIOException("Failed to write " + data, e);
            }
        }
    }

    private void writeBytes(byte[] bytes, boolean flush) {
        requireNonNull(bytes, "Data must node be null");
        synchronized (this) {
            ByteBuffer buffer = pooled.getResource().clear();
            try {
                buffer.put(bytes);
                buffer.flip();
                write(buffer, flush);
            } catch (IOException e) {
                throw new RuntimeIOException("Failed to write data", e);
            }
        }
    }

    private void write(ByteBuffer buffer, boolean flush) throws IOException {
        var sink = connection.getSinkChannel();
        if (!sink.isOpen()) {
            throw new IllegalStateException("Closed channel");
        }
        synchronized (this) {
            Channels.writeBlocking(sink, buffer);
            if (flush) {
                Channels.flushBlocking(sink);
            }
        }
        incrementMessageSent();
    }

    @Override
    public void close() {
        try {
            Channels.flushBlocking(connection.getSinkChannel());
            connection.getSourceChannel().shutdownReads();
        } catch (Exception e) {
            log.warn("Failed to flush buffer when closing", e);
        }

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


    private class RpcProxyHandler implements InvocationHandler {

        private final int timeoutMillis;
        private final boolean invokeVoidAsync;

        private RpcProxyHandler(int timeoutMillis, boolean invokeVoidAsync) {
            this.timeoutMillis = timeoutMillis;
            this.invokeVoidAsync = invokeVoidAsync;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            String methodName = method.getName();
            if (Void.TYPE.equals(method.getReturnType()) && invokeVoidAsync) {
                invokeAsync(methodName, args);
                return null;
            }
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
