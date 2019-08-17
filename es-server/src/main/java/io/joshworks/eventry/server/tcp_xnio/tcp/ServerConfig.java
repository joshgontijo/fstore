package io.joshworks.eventry.server.tcp_xnio.tcp;

import io.joshworks.fstore.core.util.Size;
import org.xnio.Option;
import org.xnio.OptionMap;
import org.xnio.Options;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class ServerConfig {

    private static final Consumer<TcpConnection> NO_OP = it -> {
    };

    private final OptionMap.Builder options = OptionMap.builder()
            .set(Options.WORKER_IO_THREADS, 5)
            .set(Options.WORKER_TASK_CORE_THREADS, 3)
            .set(Options.WORKER_TASK_MAX_THREADS, 3)
            .set(Options.WORKER_NAME, "tcp-worker")
            .set(Options.KEEP_ALIVE, true);


    private Consumer<TcpConnection> onOpen = NO_OP;
    private Consumer<TcpConnection> onClose = NO_OP;
    private Consumer<TcpConnection> onIdle = NO_OP;
    private EventHandler handler;
    private long timeout = -1;
    private int bufferSize = Size.MB.ofInt(1);
    private final Set<Class> registeredTypes = new HashSet<>();

    ServerConfig() {

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
        this.onOpen = onOpen;
        return this;
    }

    public ServerConfig onClose(Consumer<TcpConnection> onClose) {
        this.onClose = onClose;
        return this;
    }

    public ServerConfig idleTimeout(long timeout, TimeUnit unit) {
        this.timeout = unit.toMillis(timeout);
        return this;
    }

    public ServerConfig onIdle(Consumer<TcpConnection> onIdle) {
        this.onIdle = onIdle;
        return this;
    }

    public ServerConfig onEvent(EventHandler handler) {
        this.handler = handler;
        return this;
    }

    public ServerConfig registerTypes(Class<?>... types) {
        registeredTypes.addAll(Arrays.asList(types));
        return this;
    }

    public XTcpServer start(InetSocketAddress bindAddress) {
        return new XTcpServer(options.getMap(), bindAddress, registeredTypes, bufferSize, timeout, onOpen, onClose, onIdle, handler);
    }
}
