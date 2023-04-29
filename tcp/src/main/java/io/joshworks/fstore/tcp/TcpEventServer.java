package io.joshworks.fstore.tcp;

import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.tcp.codec.Compression;
import io.joshworks.fstore.tcp.conduits.BytesReceivedStreamSourceConduit;
import io.joshworks.fstore.tcp.conduits.BytesSentStreamSinkConduit;
import io.joshworks.fstore.tcp.conduits.CodecConduit;
import io.joshworks.fstore.tcp.conduits.ConduitPipeline;
import io.joshworks.fstore.tcp.conduits.FramingMessageSourceConduit;
import io.joshworks.fstore.tcp.conduits.IdleTimeoutConduit;
import io.joshworks.fstore.tcp.internal.ResponseTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xnio.ChannelListener;
import org.xnio.IoUtils;
import org.xnio.Option;
import org.xnio.OptionMap;
import org.xnio.Options;
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
    private final Compression compression;

    public TcpEventServer(
            OptionMap options,
            InetSocketAddress bindAddress,
            int maxMessageSize,
            long idleTimeout,
            Compression compression,
            int capacity,
            boolean async,
            Consumer<TcpConnection> onOpen,
            Consumer<TcpConnection> onClose,
            Consumer<TcpConnection> onIdle,
            EventHandler handler) {

        this.idleTimeout = idleTimeout;
        this.compression = compression;
        this.onConnect = onOpen;
        this.onClose = onClose;
        this.onIdle = onIdle;
        this.handler = handler;

        BufferPool pool = BufferPool.defaultPool(capacity, maxMessageSize, false);
        Acceptor acceptor = new Acceptor(idleTimeout, pool, async);

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

    public static Builder create() {
        return new Builder();
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
            onClose.accept(tcpConnection);
        } catch (Exception e) {
            log.warn("Failed to handle on onClose", e);
        }
    }

    private void onIdle(StreamConnection connection) {
        TcpConnection conn = connections.get(connection);
        if (conn != null) {
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
            tcpc.send(data, true);
        }
    }

    public void broadcast(ByteBuffer buffer) {
        for (TcpConnection tcpc : connections.values()) {
            tcpc.send(buffer.slice(), false);
        }
    }

    public void awaitTermination() throws InterruptedException {
        worker.awaitTermination();
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
        private Compression compression = Compression.NONE;
        private int capacity = 256;
        private boolean async;

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

        public Builder bufferPoolCapacity(int capacity) {
            if (bufferSize <= 0) {
                throw new IllegalArgumentException("Buffer size must be greater than zero");
            }
            this.capacity = capacity;
            return this;
        }

        public Builder handleAsync() {
            this.async = true;
            return this;
        }

        public Builder compression(Compression compression) {
            this.compression = requireNonNull(compression);
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
                    compression,
                    capacity,
                    async,
                    onOpen,
                    onClose,
                    onIdle,
                    handler);
        }
    }

    private class Acceptor implements ChannelListener<AcceptingChannel<StreamConnection>> {

        private final long timeout;
        private final BufferPool pool;
        private final boolean async;

        Acceptor(long timeout, BufferPool pool, boolean async) {
            this.timeout = timeout;
            this.pool = pool;
            this.async = async;
        }

        @Override
        public void handleEvent(AcceptingChannel<StreamConnection> channel) {
            StreamConnection conn = null;
            try {
                while ((conn = channel.accept()) != null) {
                    var tcpConnection = new TcpConnection(conn, pool, responseTable, compression);
                    connections.put(conn, tcpConnection);

                    if (timeout > 0) {
                        //adds to both source and sink channels
                        var idleTimeoutConduit = new IdleTimeoutConduit(conn, TcpEventServer.this::onIdle);
                        idleTimeoutConduit.setIdleTimeout(timeout);
                    }

                    ConduitPipeline pipeline = new ConduitPipeline(conn);
                    pipeline.closeListener(TcpEventServer.this::onClose);

                    //---------- source
                    pipeline.addStreamSource(conduit -> new BytesReceivedStreamSourceConduit(conduit, tcpConnection::updateBytesReceived));
                    pipeline.addMessageSource(conduit -> {
                        var framing = new FramingMessageSourceConduit(conduit, pool);
                        return new CodecConduit(framing, pool, tcpConnection::updateDecompressedBytes);
                    });

                    //---------- sink
//                    pipeline.addMessageSink(conduit -> new FramingMessageSinkConduit(conduit, pool));
                    pipeline.addStreamSink(conduit -> new BytesSentStreamSinkConduit(conduit, tcpConnection::updateBytesSent));

                    //---------- listeners
                    EventHandler keepAliveHandler = new KeepAliveHandler(handler);
                    ReadListener readListener = new ReadListener(tcpConnection, keepAliveHandler, async);

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

}
