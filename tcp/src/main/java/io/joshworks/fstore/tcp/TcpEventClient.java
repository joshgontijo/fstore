package io.joshworks.fstore.tcp;

import io.joshworks.fstore.core.RuntimeIOException;
import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.tcp.codec.Compression;
import io.joshworks.fstore.tcp.conduits.BytesReceivedStreamSourceConduit;
import io.joshworks.fstore.tcp.conduits.BytesSentStreamSinkConduit;
import io.joshworks.fstore.tcp.conduits.CodecConduit;
import io.joshworks.fstore.tcp.conduits.ConduitPipeline;
import io.joshworks.fstore.tcp.conduits.FramingMessageSourceConduit;
import io.joshworks.fstore.tcp.conduits.KeepAliveConduit;
import io.joshworks.fstore.tcp.internal.ResponseTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xnio.ChannelListener;
import org.xnio.IoFuture;
import org.xnio.Option;
import org.xnio.OptionMap;
import org.xnio.Options;
import org.xnio.StreamConnection;
import org.xnio.Xnio;
import org.xnio.XnioWorker;

import java.net.InetSocketAddress;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

public class TcpEventClient {

    private static final Logger log = LoggerFactory.getLogger(TcpEventClient.class);

    private final InetSocketAddress bindAddress;
    private final long keepAliveInterval;
    private final Consumer<TcpConnection> onClose;
    private final EventHandler eventHandler;
    private final XnioWorker worker;

    private final BufferPool pool;
    private final CountDownLatch connectLatch = new CountDownLatch(1);
    private final ResponseTable responseTable = new ResponseTable();
    private final Compression compression;
    private final boolean async;
    private transient TcpConnection tcpConnection;

    private TcpEventClient(OptionMap options,
                           InetSocketAddress bindAddress,
                           int maxMessageSize,
                           long keepAliveInterval,
                           Compression compression,
                           int capacity,
                           boolean async,
                           Consumer<TcpConnection> onClose,
                           EventHandler handler) {
        this.compression = compression;
        this.async = async;

        this.pool = BufferPool.defaultPool(capacity, maxMessageSize, false);

        this.bindAddress = bindAddress;
        this.keepAliveInterval = keepAliveInterval;
        this.onClose = onClose;
        this.eventHandler = handler;


        try {
            final Xnio xnio = Xnio.getInstance();
            this.worker = xnio.createWorker(options);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Builder create() {
        return new Builder();
    }

    private synchronized TcpConnection connect(long timeout, TimeUnit unit) {
        try {
            IoFuture<StreamConnection> connFuture = worker.openStreamConnection(bindAddress, new ConnectionAccepted(), OptionMap.builder().set(Options.WORKER_IO_THREADS, 1).getMap());
            IoFuture.Status status = connFuture.awaitInterruptibly(timeout, unit);
            if (IoFuture.Status.FAILED.equals(status)) {
                throw connFuture.getException();
            }
            if (!connectLatch.await(timeout, unit)) {
                throw new TimeoutException("Connection timeout: " + bindAddress.getAddress().getHostAddress());
            }
            if (tcpConnection == null) { //should never happen
                throw new RuntimeIOException("Failed to connect: Expected valid tcp connection");
            }
            log.info("Connected to {}", tcpConnection.peerAddress());
            return tcpConnection;

        } catch (Exception e) {
            worker.shutdown();
            throw new RuntimeException("Failed to connect", e);
        }
    }

    public static final class Builder {

        private final OptionMap.Builder options = OptionMap.builder()
                .set(Options.WORKER_IO_THREADS, 1)
                .set(Options.WORKER_TASK_CORE_THREADS, 5)
                .set(Options.WORKER_TASK_MAX_THREADS, 5)
                .set(Options.WORKER_NAME, "tcp-client");


        private Consumer<TcpConnection> onClose = conn -> log.info("Connection {} closed", conn.peerAddress());
        private EventHandler handler = new DiscardEventHandler();
        private long keepAliveInterval = -1;
        private int bufferSize = Size.MB.ofInt(1);
        private Compression compression = Compression.NONE;
        private int capacity = 256;
        private boolean async = false;

        private Builder() {

        }

        public <T> Builder option(Option<T> key, T value) {
            options.set(key, value);
            return this;
        }

        public Builder name(String name) {
            this.options.set(Options.WORKER_NAME, requireNonNull(name));
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

        /**
         * Maximum event size
         */
        public Builder maxMessageSize(int bufferSize) {
            if (bufferSize <= 0) {
                throw new IllegalArgumentException("Buffer size must be greater than zero");
            }
            this.bufferSize = bufferSize;
            return this;
        }

        public Builder bufferPoolCapacity(int capacity) {
            this.capacity = capacity;
            return this;
        }

        public Builder onClose(Consumer<TcpConnection> onClose) {
            this.onClose = onClose;
            return this;
        }

        /**
         * The frequency to send KEEP_ALIVE messages to the server.
         */
        public Builder keepAlive(long timeout, TimeUnit unit) {
            this.keepAliveInterval = unit.toMillis(timeout);
            return this;
        }

        public Builder onEvent(EventHandler handler) {
            this.handler = requireNonNull(handler);
            return this;
        }

        public TcpConnection connect(InetSocketAddress bindAddress, long timeout, TimeUnit unit) {
            TcpEventClient client = new TcpEventClient(options.getMap(),
                    bindAddress,
                    bufferSize,
                    keepAliveInterval,
                    compression,
                    capacity,
                    async,
                    onClose,
                    handler);

            return client.connect(timeout, unit);
        }
    }

    private class ConnectionAccepted implements ChannelListener<StreamConnection> {

        @Override
        public void handleEvent(StreamConnection channel) {
            tcpConnection = new TcpConnection(channel, pool, responseTable, compression);

            channel.setCloseListener(conn -> onClose.accept(tcpConnection));

            ConduitPipeline pipeline = new ConduitPipeline(channel);
            pipeline.addStreamSource(conduit -> new BytesReceivedStreamSourceConduit(conduit, tcpConnection::updateBytesReceived));
            pipeline.addMessageSource(conduit -> {
                var framing = new FramingMessageSourceConduit(conduit, pool);
                return new CodecConduit(framing, pool, tcpConnection::updateDecompressedBytes);
            });

//            pipeline.addMessageSink(conduit -> new FramingMessageSinkConduit(conduit, pool));
            pipeline.addStreamSink(conduit -> new BytesSentStreamSinkConduit(conduit, tcpConnection::updateBytesSent));

            if (keepAliveInterval > 0) {
                new KeepAliveConduit(channel, keepAliveInterval); //adds to source and sink
            }

            ReadListener readListener = new ReadListener(tcpConnection, eventHandler, async);
            pipeline.readListener(readListener);

            channel.setCloseListener(conn -> responseTable.clear());

            channel.getSourceChannel().resumeReads();
            channel.getSinkChannel().resumeWrites();
            connectLatch.countDown();
        }
    }


}
