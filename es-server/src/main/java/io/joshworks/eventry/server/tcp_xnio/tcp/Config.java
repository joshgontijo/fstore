package io.joshworks.eventry.server.tcp_xnio.tcp;

import io.joshworks.fstore.core.io.buffers.BufferPool;
import org.xnio.Option;
import org.xnio.OptionMap;
import org.xnio.Options;
import org.xnio.StreamConnection;
import org.xnio.Xnio;
import org.xnio.XnioWorker;
import org.xnio.channels.AcceptingChannel;

import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

public class Config {

    private final OptionMap.Builder options = OptionMap.builder()
            .set(Options.WORKER_IO_THREADS, 5)
            .set(Options.WORKER_TASK_CORE_THREADS, 3)
            .set(Options.WORKER_TASK_MAX_THREADS, 3)
            .set(Options.WORKER_NAME, "josh-worker")
            .set(Options.KEEP_ALIVE, true);

    Consumer<StreamConnection> onConnect;
    Consumer<StreamConnection> onClose;
    EventHandler handler;
    long timeout;
    InetSocketAddress bindAddress;
    int maxBufferSize;

    public <T> Config option(Option<T> key, T value) {
        options.set(key, value);
        return this;
    }

    /**
     * The read buffer size, this determines the max entry size the channel can accept.
     * If value is equals or less than zero, then the buffer will expand to fit the largest entry,
     * which is CPU and memory inefficient
     */
    public Config bufferSize(int bufferSize) {
        this.maxBufferSize = bufferSize;
        return this;
    }

    public Config onConnect(Consumer<StreamConnection> handler) {
        this.onConnect = handler;
        return this;
    }

    public Config onDisconnect(Object handler) {
        this.onConnect = onConnect;
        return this;
    }

    public Config idleTimeout(long timeout, TimeUnit unit) {
        this.timeout = unit.toMillis(timeout);
        return this;
    }

    public Config eventHandler(EventHandler handler) {
        this.handler = handler;
        return this;
    }

    public Config bindAddress(InetSocketAddress bindAddress) {
        this.bindAddress = requireNonNull(bindAddress);
        return this;
    }

    public XTcpServer connect() {
        try {
            final XnioWorker worker = Xnio.getInstance().createWorker(options.getMap());
            BufferPool bufferPool = new BufferPool(maxBufferSize, true);
            // OptionsMap is the override
            AcceptingChannel<StreamConnection> channel = worker.createStreamConnectionServer(bindAddress, new Acceptor(this, bufferPool), OptionMap.EMPTY);
            channel.resumeAccepts();
            return new XTcpServer(worker, channel);
        } catch (Exception e) {
            throw new RuntimeException("Failed to start TCP server", e);
        }
    }
}
