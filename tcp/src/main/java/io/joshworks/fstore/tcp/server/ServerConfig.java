package io.joshworks.fstore.tcp.server;

import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.tcp.EventHandler;
import io.joshworks.fstore.tcp.TcpConnection;
import io.joshworks.fstore.tcp.TcpMessageServer;
import org.xnio.Option;
import org.xnio.OptionMap;
import org.xnio.Options;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

public class ServerConfig {

    private final OptionMap.Builder options = OptionMap.builder().set(Options.WORKER_NAME, "tcp-server");

    private Consumer<TcpConnection> onOpen = conn -> TcpConnection.logger.info("Connection {} opened", conn.peerAddress());
    private Consumer<TcpConnection> onClose = conn -> TcpConnection.logger.info("Connection {} closed", conn.peerAddress());
    private Consumer<TcpConnection> onIdle = conn -> TcpConnection.logger.info("Connection {} is idle", conn.peerAddress());
    private EventHandler handler = new DiscardEventHandler();
    private long timeout = -1;
    private int bufferSize = Size.KB.ofInt(256);
    private boolean async;
    private int maxBuffers;
    private int writePoolSize = 10;
    private int readPoolSize = 10;

    public ServerConfig() {

    }

    public <T> ServerConfig option(Option<T> key, T value) {
        options.set(key, value);
        return this;
    }

    public ServerConfig readBufferPool(int readPoolSize) {
        this.readPoolSize = readPoolSize;
        return this;
    }

    public ServerConfig writeBufferPool(int writePoolSize) {
        this.writePoolSize = writePoolSize;
        return this;
    }

    /**
     * Maximum event size
     */
    public ServerConfig maxEntrySize(int bufferSize) {
        this.bufferSize = bufferSize;
        return this;
    }

    public ServerConfig onOpen(Consumer<TcpConnection> onOpen) {
        this.onOpen = requireNonNull(onOpen);
        return this;
    }

    public ServerConfig onClose(Consumer<TcpConnection> onClose) {
        this.onClose = requireNonNull(onClose);
        return this;
    }

    public ServerConfig idleTimeout(long timeout, TimeUnit unit) {
        this.timeout = unit.toMillis(timeout);
        return this;
    }

    public ServerConfig onIdle(Consumer<TcpConnection> onIdle) {
        this.onIdle = requireNonNull(onIdle);
        return this;
    }

    public ServerConfig onEvent(EventHandler handler) {
        this.handler = handler;
        return this;
    }

    public ServerConfig asyncHandler() {
        this.async = true;
        return this;
    }

    public TcpMessageServer start(InetSocketAddress bindAddress) {
        return new TcpMessageServer(
                options.getMap(),
                bindAddress,
                readPoolSize,
                writePoolSize,
                bufferSize,
                timeout,
                onOpen,
                onClose,
                onIdle,
                async,
                handler);
    }
}
