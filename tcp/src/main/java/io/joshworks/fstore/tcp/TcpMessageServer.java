package io.joshworks.fstore.tcp;

import io.joshworks.fstore.core.io.buffers.StupidPool;
import io.joshworks.fstore.tcp.conduits.BytesReceivedStreamSourceConduit;
import io.joshworks.fstore.tcp.conduits.BytesSentStreamSinkConduit;
import io.joshworks.fstore.tcp.conduits.ConduitPipeline;
import io.joshworks.fstore.tcp.conduits.FramingMessageSinkConduit;
import io.joshworks.fstore.tcp.conduits.FramingMessageSourceConduit;
import io.joshworks.fstore.tcp.conduits.IdleTimeoutConduit;
import io.joshworks.fstore.tcp.server.KeepAliveHandler;
import io.joshworks.fstore.tcp.server.ServerConfig;
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
    private final int maxMessageSize;
    private final long idleTimeout;
    private final boolean async;
    private final Consumer<TcpConnection> onConnect;
    private final Consumer<TcpConnection> onClose;
    private final Consumer<TcpConnection> onIdle;
    private final EventHandler handler;

    private final AtomicBoolean closed = new AtomicBoolean();

    private Map<StreamConnection, TcpConnection> connections = new ConcurrentHashMap<>();

    public TcpMessageServer(
            OptionMap options,
            InetSocketAddress bindAddress,
            int readPoolSize,
            int writePoolSize,
            int maxMessageSize,
            long idleTimeout,
            Consumer<TcpConnection> onOpen,
            Consumer<TcpConnection> onClose,
            Consumer<TcpConnection> onIdle,
            boolean async,
            EventHandler handler) {

        this.maxMessageSize = maxMessageSize;
        this.idleTimeout = idleTimeout;
        this.onConnect = onOpen;
        this.onClose = onClose;
        this.onIdle = onIdle;
        this.async = async;
        this.handler = handler;

        StupidPool readPool = new StupidPool(readPoolSize, maxMessageSize);
        StupidPool writePool = new StupidPool(writePoolSize, maxMessageSize);

        Acceptor acceptor = new Acceptor(idleTimeout, readPool, writePool);
        try {
            this.worker = Xnio.getInstance().createWorker(options);
            this.channel = connect(bindAddress, acceptor, options);
        } catch (Exception e) {
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
            tcpc.send(data); //FIXME need to slice when ByteBuffer type
        }
    }

    public void awaitTermination() throws InterruptedException {
        worker.awaitTermination();
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
        private final StupidPool writePool;
        private final StupidPool readPool;

        Acceptor(long timeout, StupidPool readPool, StupidPool writePool) {
            this.timeout = timeout;
            this.readPool = readPool;
            this.writePool = writePool;
        }

        @Override
        public void handleEvent(AcceptingChannel<StreamConnection> channel) {
            StreamConnection conn = null;
            try {
                while ((conn = channel.accept()) != null) {
                    var tcpConnection = new TcpConnection(conn, writePool);
                    connections.put(conn, tcpConnection);
                    logger.info("Connection accepted: {}", tcpConnection.peerAddress());

                    if (timeout > 0) {
                        //adds to both source and sink channels
                        var idleTimeoutConduit = new IdleTimeoutConduit(conn, TcpMessageServer.this::onIdle);
                        idleTimeoutConduit.setIdleTimeout(timeout);
                    }

                    ConduitPipeline pipeline = new ConduitPipeline(conn);
                    pipeline.closeListener(TcpMessageServer.this::onClose);
                    //---------- source
                    pipeline.addMessageSource(conduit -> new FramingMessageSourceConduit(conduit, maxMessageSize, readPool));
                    pipeline.addStreamSource(conduit -> new BytesReceivedStreamSourceConduit(conduit, tcpConnection::updateBytesReceived));

                    //---------- sink
                    pipeline.addStreamSink(conduit -> new BytesSentStreamSinkConduit(conduit, tcpConnection::updateBytesSent));
                    pipeline.addStreamSink(FramingMessageSinkConduit::new);

                    //---------- listeners
                    EventHandler keepAliveHandler = new KeepAliveHandler(handler);
                    ReadHandler readHandler = new ReadHandler(tcpConnection, keepAliveHandler, async);

                    pipeline.readListener(readHandler);

                    conn.getSourceChannel().resumeReads();
                    conn.getSinkChannel().resumeWrites();

                    onConnect(tcpConnection);
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
