package io.joshworks.fstore.tcp;

import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.tcp.conduits.BytesReceivedStreamSourceConduit;
import io.joshworks.fstore.tcp.conduits.BytesSentStreamSinkConduit;
import io.joshworks.fstore.tcp.conduits.ConduitPipeline;
import io.joshworks.fstore.tcp.conduits.FramingMessageSinkConduit;
import io.joshworks.fstore.tcp.conduits.FramingMessageSourceConduit;
import io.joshworks.fstore.tcp.conduits.IdleTimeoutConduit;
import io.joshworks.fstore.tcp.handlers.DiscardEventHandler;
import io.joshworks.fstore.tcp.handlers.EventHandler;
import io.joshworks.fstore.tcp.internal.ResponseTable;
import io.joshworks.fstore.tcp.server.KeepAliveHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xnio.BufferAllocator;
import org.xnio.ByteBufferSlicePool;
import org.xnio.ChannelListener;
import org.xnio.IoUtils;
import org.xnio.Option;
import org.xnio.OptionMap;
import org.xnio.Options;
import org.xnio.Pool;
import org.xnio.StreamConnection;
import org.xnio.Xnio;
import org.xnio.XnioWorker;
import org.xnio.channels.AcceptingChannel;
import org.xnio.channels.Channels;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

/**
 * A Message server, it uses length prefixed message format to parse messages down to the pipeline
 */
public class TcpEventServer implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(TcpEventServer.class);

    private final XnioWorker worker;
    private final AcceptingChannel<StreamConnection> channel;
    private final long idleTimeout;
    private final Consumer<TcpConnection> onConnect;
    private final Consumer<TcpConnection> onClose;
    private final Consumer<TcpConnection> onIdle;
    private final EventHandler handler;
    private final ResponseTable responseTable = new ResponseTable();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Map<StreamConnection, TcpConnection> connections = new ConcurrentHashMap<>();

    public TcpEventServer(
            OptionMap options,
            InetSocketAddress bindAddress,
            int maxMessageSize,
            long idleTimeout,
            Consumer<TcpConnection> onOpen,
            Consumer<TcpConnection> onClose,
            Consumer<TcpConnection> onIdle,
            EventHandler handler) {

        this.idleTimeout = idleTimeout;
        this.onConnect = onOpen;
        this.onClose = onClose;
        this.onIdle = onIdle;
        this.handler = handler;

        int bufferPerRegion = 128;
        int regionSize = maxMessageSize * bufferPerRegion;
        Pool<ByteBuffer> messagePool = new ByteBufferSlicePool(BufferAllocator.BYTE_BUFFER_ALLOCATOR, maxMessageSize, regionSize);
        Acceptor acceptor = new Acceptor(idleTimeout, messagePool);

        XnioWorker worker = null;
        try {
            this.worker = worker = Xnio.getInstance().createWorker(options);
            this.channel = connect(bindAddress, acceptor, options);
        } catch (Exception e) {
            if (worker != null) {
                worker.shutdownNow();
            }
            throw new RuntimeException("Failed to start server", e);
        }
    }

    private AcceptingChannel<StreamConnection> connect(InetSocketAddress bindAddress, Acceptor acceptor, OptionMap options) throws IOException {
        AcceptingChannel<StreamConnection> conn = worker.createStreamConnectionServer(bindAddress, acceptor, options);
        conn.resumeAccepts();
        return conn;
    }

    private void onClose(StreamConnection conn) {
        try {
            TcpConnection tcpConnection = connections.remove(conn);
            if (tcpConnection == null) {
                return;
            }
            log.info("Connection closed: {}", tcpConnection.peerAddress());
            onClose.accept(tcpConnection);
        } catch (Exception e) {
            log.warn("Failed to handle on onClose", e);
        }
    }

    private void onIdle(StreamConnection connection) {
        TcpConnection conn = connections.get(connection);
        if (conn != null) {
            log.info("Closed idle connection to {} after {}ms of inactivity", conn.peerAddress(), idleTimeout);
            onIdle.accept(conn);
        }
    }

    private void onConnect(TcpConnection connection) {
        try {
            onConnect.accept(connection);
        } catch (Exception e) {
            log.error("Error while handling onConnect, connection will be closed", e);
            throw new IllegalStateException(e);
        }
    }

    public int connections() {
        return connections.size();
    }

    public long idleTimeout() {
        return idleTimeout;
    }

    public Map<StreamConnection, TcpConnection> printConnections() {
        return connections;
    }

    public void broadcast(Object data) {
        for (TcpConnection tcpc : connections.values()) {
            tcpc.send(data);
        }
    }

    public void broadcast(ByteBuffer buffer) {
        for (TcpConnection tcpc : connections.values()) {
            tcpc.send(buffer.slice());
        }
    }

    public void awaitTermination() throws InterruptedException {
        worker.awaitTermination();
    }

    public static Builder create() {
        return new Builder();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        channel.suspendAccepts();
        for (TcpConnection connection : connections.values()) {
            connection.close();
        }
        connections.clear();
        channel.getWorker().shutdown();
        IoUtils.safeClose(channel);
    }

    private class Acceptor implements ChannelListener<AcceptingChannel<StreamConnection>> {

        private final long timeout;
        private final Pool<ByteBuffer> messagePool;

        Acceptor(long timeout, Pool<ByteBuffer> messagePool) {
            this.timeout = timeout;
            this.messagePool = messagePool;
        }

        @Override
        public void handleEvent(AcceptingChannel<StreamConnection> channel) {
            StreamConnection conn = null;
            try {
                while ((conn = channel.accept()) != null) {
                    var tcpConnection = new TcpConnection(conn, messagePool, responseTable);
                    connections.put(conn, tcpConnection);
                    log.info("Connection accepted: {}", tcpConnection.peerAddress());

                    if (timeout > 0) {
                        //adds to both source and sink channels
                        var idleTimeoutConduit = new IdleTimeoutConduit(conn, TcpEventServer.this::onIdle);
                        idleTimeoutConduit.setIdleTimeout(timeout);
                    }

                    ConduitPipeline pipeline = new ConduitPipeline(conn);
                    pipeline.closeListener(TcpEventServer.this::onClose);
                    //---------- source
                    pipeline.addMessageSource(conduit -> new FramingMessageSourceConduit(conduit, messagePool.allocate()));
                    pipeline.addStreamSource(conduit -> new BytesReceivedStreamSourceConduit(conduit, tcpConnection::updateBytesReceived));

                    //---------- sink
                    pipeline.addMessageSink(conduit -> new FramingMessageSinkConduit(conduit, messagePool.allocate()));
                    pipeline.addStreamSink(conduit -> new BytesSentStreamSinkConduit(conduit, tcpConnection::updateBytesSent));

                    //---------- listeners
                    EventHandler responseHandler = new ResponseHandler(handler, responseTable);
                    EventHandler keepAliveHandler = new KeepAliveHandler(responseHandler);
                    ReadListener readListener = new ReadListener(tcpConnection, keepAliveHandler, messagePool);

                    pipeline.readListener(readListener);

                    conn.getSourceChannel().resumeReads();
                    conn.getSinkChannel().resumeWrites();

                    onConnect(tcpConnection);
                }
            } catch (Exception e) {
                log.error("Failed to accept connection", e);
                if (conn != null) {
                    try {
                        Channels.flushBlocking(conn.getSinkChannel());
                    } catch (IOException ignore) {
                    }
                    IoUtils.safeClose(conn);
                    connections.remove(conn);
                }
            }
        }
    }

    public static class Builder {

        private final OptionMap.Builder options = OptionMap.builder()
                .set(Options.WORKER_IO_THREADS, 1)
                .set(Options.WORKER_TASK_CORE_THREADS, 5)
                .set(Options.WORKER_TASK_MAX_THREADS, 5)
                .set(Options.WORKER_NAME, "tcp-server");

        private Consumer<TcpConnection> onOpen = conn -> log.info("Connection {} opened", conn.peerAddress());
        private Consumer<TcpConnection> onClose = conn -> log.info("Connection {} closed", conn.peerAddress());
        private Consumer<TcpConnection> onIdle = conn -> log.info("Connection {} is idle", conn.peerAddress());
        private EventHandler handler = new DiscardEventHandler();
        private long timeout = -1;
        private int bufferSize = Size.KB.ofInt(64);

        public Builder() {

        }

        public Builder name(String name) {
            this.options.set(Options.WORKER_NAME, requireNonNull(name));
            return this;
        }

        public <T> Builder option(Option<T> key, T value) {
            options.set(key, value);
            return this;
        }

        /**
         * Maximum event size
         */
        public Builder maxMessageSize(int maxEventSize) {
            if (bufferSize <= 0) {
                throw new IllegalArgumentException("Buffer size must be greater than zero");
            }
            this.bufferSize = maxEventSize;
            return this;
        }

        public Builder idleTimeout(long timeout, TimeUnit unit) {
            timeout = unit.toMillis(timeout);
            if (timeout <= 0) {
                throw new IllegalArgumentException("Idle timeout bust be greater than zero");
            }
            this.timeout = timeout;
            return this;
        }

        public Builder onOpen(Consumer<TcpConnection> onOpen) {
            this.onOpen = requireNonNull(onOpen);
            return this;
        }

        public Builder onClose(Consumer<TcpConnection> onClose) {
            this.onClose = requireNonNull(onClose);
            return this;
        }

        public Builder onIdle(Consumer<TcpConnection> onIdle) {
            this.onIdle = requireNonNull(onIdle);
            return this;
        }

        public Builder onEvent(EventHandler handler) {
            this.handler = requireNonNull(handler);
            return this;
        }

        public TcpEventServer start(InetSocketAddress bindAddress) {
            return new TcpEventServer(
                    options.getMap(),
                    bindAddress,
                    bufferSize,
                    timeout,
                    onOpen,
                    onClose,
                    onIdle,
                    handler);
        }
    }

}
