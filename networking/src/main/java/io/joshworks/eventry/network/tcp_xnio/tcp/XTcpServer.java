package io.joshworks.eventry.network.tcp_xnio.tcp;

import io.joshworks.eventry.network.tcp_xnio.tcp.conduits.BytesReceivedStreamSourceConduit;
import io.joshworks.eventry.network.tcp_xnio.tcp.conduits.BytesSentStreamSinkConduit;
import io.joshworks.eventry.network.tcp_xnio.tcp.conduits.ConduitPipeline;
import io.joshworks.eventry.network.tcp_xnio.tcp.conduits.FramingMessageSourceConduit;
import io.joshworks.eventry.network.tcp_xnio.tcp.conduits.IdleTimeoutConduit;
import io.joshworks.eventry.network.tcp_xnio.tcp.internal.KeepAlive;
import io.joshworks.fstore.core.io.buffers.SimpleBufferPool;
import io.joshworks.fstore.serializer.kryo.KryoStoreSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xnio.ByteBufferSlicePool;
import org.xnio.ChannelListener;
import org.xnio.IoUtils;
import org.xnio.OptionMap;
import org.xnio.Pooled;
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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * A Message server, it uses length prefixed message format to parse messages down to the pipeline
 */
public class XTcpServer implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(XTcpServer.class);

    private final XnioWorker worker;
    private final AcceptingChannel<StreamConnection> channel;
    private final long idleTimeout;
    private final Consumer<TcpConnection> onConnect;
    private final Consumer<TcpConnection> onClose;
    private final Consumer<TcpConnection> onIdle;
    private final EventHandler handler;

    private final KryoStoreSerializer serializer;

    private final SimpleBufferPool messagePool;
    private final ByteBufferSlicePool readPool;

    private final AtomicBoolean closed = new AtomicBoolean();

    private Map<StreamConnection, TcpConnection> connections = new ConcurrentHashMap<>();

    public XTcpServer(
            OptionMap options,
            InetSocketAddress bindAddress,
            Set<Class> registeredTypes,
            int maxBufferSize,
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

        this.messagePool = new SimpleBufferPool(maxBufferSize, true);
        this.readPool = new ByteBufferSlicePool(maxBufferSize, maxBufferSize * 50);

        registeredTypes.add(KeepAlive.class);
        this.serializer = KryoStoreSerializer.register(registeredTypes.toArray(Class[]::new));

        Acceptor acceptor = new Acceptor(idleTimeout, readPool, messagePool);
        try {
            this.worker = Xnio.getInstance().createWorker(options);
            this.channel = connect(bindAddress, acceptor);
        } catch (Exception e) {
            throw new RuntimeException("Failed to start server", e);
        }
    }

    private AcceptingChannel<StreamConnection> connect(InetSocketAddress bindAddress, Acceptor acceptor) throws IOException {
        AcceptingChannel<StreamConnection> conn = worker.createStreamConnectionServer(bindAddress, acceptor, OptionMap.EMPTY); //OptionMap.EMPTY -> override
        conn.resumeAccepts();
        return conn;
    }

    private void onClose(StreamConnection conn) {
        try {
            TcpConnection tcpConnection = connections.remove(conn);
            if (tcpConnection == null) {
                return;
            }
            logger.info("Connection closed: {}", tcpConnection.peerAddress());
            onClose.accept(tcpConnection);
        } catch (Exception e) {
            logger.warn("Failed to handle on onClose", e);
        }
    }

    private void onIdle(StreamConnection connection) {
        TcpConnection tcpc = connections.get(connection);
        if (tcpc != null) {
            logger.info("Closing connection idle ({}ms) connection: {}", idleTimeout, tcpc.peerAddress());
            onIdle.accept(tcpc);
        }
    }

    private void onConnect(TcpConnection connection) {
        try {
            onConnect.accept(connection);
        } catch (Exception e) {
            logger.error("Error while handling onConnect, connection will be closed", e);
            throw new IllegalStateException(e);
        }
    }

    public int connections() {
        return connections.size();
    }

    public long idleTimeout() {
        return idleTimeout;
    }

    public void broadcast(Object data) {
        for (TcpConnection tcpc : connections.values()) {
            tcpc.send(data);
        }
    }

    public static ServerConfig create() {
        return new ServerConfig();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        IoUtils.safeClose(channel);
    }

    private class Acceptor implements ChannelListener<AcceptingChannel<StreamConnection>> {

        private final long timeout;
        private final SimpleBufferPool messagePool;
        private final ByteBufferSlicePool readPool;

        Acceptor(long timeout, ByteBufferSlicePool readPool, SimpleBufferPool messagePool) {
            this.timeout = timeout;
            this.readPool = readPool;
            this.messagePool = messagePool;
        }

        @Override
        public void handleEvent(AcceptingChannel<StreamConnection> channel) {
            StreamConnection conn = null;
            try {
                while ((conn = channel.accept()) != null) {
                    var tcpConnection = new TcpServerConnection(conn, connections, messagePool);
                    connections.put(conn, tcpConnection);
                    logger.info("Connection accepted: {}", tcpConnection.peerAddress());

                    Pooled<ByteBuffer> polled = readPool.allocate();

                    conn.setCloseListener(sc -> {
                        polled.free();
                        XTcpServer.this.onClose(sc);
                    });

                    //adds to both source and sink channels
                    var idleTimeoutConduit = new IdleTimeoutConduit(conn, XTcpServer.this::onIdle);
                    idleTimeoutConduit.setIdleTimeout(timeout);

                    ConduitPipeline pipeline = new ConduitPipeline(conn);
                    //---------- source
                    pipeline.addMessageSource(conduit -> new FramingMessageSourceConduit(conduit, false, polled));
                    pipeline.addStreamSource(conduit -> new BytesReceivedStreamSourceConduit(conduit, tcpConnection::updateBytesReceived));

                    //---------- sink
                    pipeline.addStreamSink(conduit -> new BytesSentStreamSinkConduit(conduit, tcpConnection::updateBytesSent));

                    //---------- listeners
                    pipeline.readListener(new ReadHandler(tcpConnection, new KryoEventHandler(handler)));
                    pipeline.closeListener(sc -> {
                        polled.free();
                        XTcpServer.this.onClose(sc);
                    });

                    onConnect(tcpConnection);
                    conn.getSourceChannel().resumeReads();
                }
            } catch (Exception e) {
                logger.error("Failed to accept connection", e);
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
