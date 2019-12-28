package io.joshworks.fstore.tcp.server;

import io.joshworks.fstore.core.util.Size;
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
    private ServerEventHandler handler = new DiscardEventHandler();
    private Object rpcHandlerTarget;
    private long timeout = -1;
    private int bufferSize = Size.MB.ofInt(1);
    private boolean async;

    public ServerConfig() {

    }

    public <T> ServerConfig option(Option<T> key, T value) {
        options.set(key, value);
        return this;
    }

    /**
     * Maximum event size
     */
    public ServerConfig bufferSize(int bufferSize) {
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

    public ServerConfig onEvent(ServerEventHandler handler) {
        this.handler = handler;
        return this;
    }

    public ServerConfig rpcHandler(Object rpcHandler) {
        this.rpcHandlerTarget = rpcHandler;
        return this;
    }

    public ServerConfig asyncHandler() {
        this.async = true;
        return this;
    }

    public TcpMessageServer start(InetSocketAddress bindAddress) {
        return new TcpMessageServer(options.getMap(), bindAddress, bufferSize, timeout, onOpen, onClose, onIdle, async, handler, rpcHandlerTarget);
    }
}
