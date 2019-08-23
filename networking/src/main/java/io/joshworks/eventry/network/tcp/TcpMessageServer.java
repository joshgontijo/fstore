package io.joshworks.eventry.network.tcp;

import io.joshworks.eventry.network.tcp.conduits.BytesReceivedStreamSourceConduit;
import io.joshworks.eventry.network.tcp.conduits.BytesSentStreamSinkConduit;
import io.joshworks.eventry.network.tcp.conduits.ConduitPipeline;
import io.joshworks.eventry.network.tcp.conduits.FramingMessageSourceConduit;
import io.joshworks.eventry.network.tcp.conduits.IdleTimeoutConduit;
import io.joshworks.eventry.network.tcp.internal.KeepAlive;
import io.joshworks.fstore.core.io.buffers.SimpleBufferPool;
import io.joshworks.fstore.serializer.kryo.KryoStoreSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xnio.ChannelListener;
import org.xnio.IoUtils;
import org.xnio.OptionMap;
import org.xnio.StreamConnection;
import org.xnio.Xnio;
import org.xnio.XnioWorker;
import org.xnio.channels.AcceptingChannel;
import org.xnio.channels.Channels;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * A Message server, it uses length prefixed message format to parse messages down to the pipeline
 */
public class TcpMessageServer implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(TcpMessageServer.class);

    private final XnioWorker worker;
    private final AcceptingChannel<StreamConnection> channel;
    private final long idleTimeout;
    private final Consumer<TcpConnection> onConnect;
    private final Consumer<TcpConnection> onClose;
    private final Consumer<TcpConnection> onIdle;
    private final ServerEventHandler handler;

    private final KryoStoreSerializer serializer;

    private final SimpleBufferPool messagePool;
//    private final ByteBufferSlicePool readPool;

    private final SimpleBufferPool readPool;

    private final AtomicBoolean closed = new AtomicBoolean();

    private Map<StreamConnection, TcpConnection> connections = new ConcurrentHashMap<>();

    public TcpMessageServer(
            OptionMap options,
            InetSocketAddress bindAddress,
            Set<Class> registeredTypes,
            int maxBufferSize,
            long idleTimeout,
            Consumer<TcpConnection> onOpen,
            Consumer<TcpConnection> onClose,
            Consumer<TcpConnection> onIdle,
            ServerEventHandler handler) {

        this.idleTimeout = idleTimeout;
        this.onConnect = onOpen;
        this.onClose = onClose;
        this.onIdle = onIdle;
        this.handler = handler;

        this.messagePool = new SimpleBufferPool(maxBufferSize, true);
        this.readPool = new SimpleBufferPool(maxBufferSize, true);

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
            logger.info("Closed idle connection to {} after {}ms of inactivity", tcpc.peerAddress(), idleTimeout);
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

    public Map<StreamConnection, TcpConnection> printConnections() {
        return connections;
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
        private final SimpleBufferPool messagePool;
        private final SimpleBufferPool readPool;

        Acceptor(long timeout, SimpleBufferPool readPool, SimpleBufferPool messagePool) {
            this.timeout = timeout;
            this.readPool = readPool;
            this.messagePool = messagePool;
        }

        @Override
        public void handleEvent(AcceptingChannel<StreamConnection> channel) {
            StreamConnection conn = null;
            try {
                while ((conn = channel.accept()) != null) {
                    var tcpConnection = new TcpConnection(conn, messagePool);
                    connections.put(conn, tcpConnection);
                    logger.info("Connection accepted: {}", tcpConnection.peerAddress());

                    SimpleBufferPool.BufferRef polled = readPool.allocateRef();

                    conn.setCloseListener(sc -> {
                        polled.free();
                        TcpMessageServer.this.onClose(sc);
                    });

                    //adds to both source and sink channels
                    if (timeout > 0) {
                        var idleTimeoutConduit = new IdleTimeoutConduit(conn, TcpMessageServer.this::onIdle);
                        idleTimeoutConduit.setIdleTimeout(timeout);
                    }

                    ConduitPipeline pipeline = new ConduitPipeline(conn);
                    //---------- source
                    pipeline.addMessageSource(conduit -> new FramingMessageSourceConduit(conduit, polled));
                    pipeline.addStreamSource(conduit -> new BytesReceivedStreamSourceConduit(conduit, tcpConnection::updateBytesReceived));

                    //---------- sink
                    pipeline.addStreamSink(conduit -> new BytesSentStreamSinkConduit(conduit, tcpConnection::updateBytesSent));

                    //---------- listeners
                    InternalServerEventHandler serverHandler = new InternalServerEventHandler(handler);
                    pipeline.readListener(new ReadHandler(tcpConnection, serverHandler));
                    pipeline.closeListener(sc -> {
                        polled.free();
                        TcpMessageServer.this.onClose(sc);
                    });

                    onConnect(tcpConnection);
                    conn.getSourceChannel().resumeReads();
                    conn.getSinkChannel().resumeWrites();
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
