package io.joshworks.eventry.network.tcp;

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

public class TcpEventClient {

    private static final Consumer<TcpConnection> NO_OP = it -> {};

    private final OptionMap.Builder options = OptionMap.builder()
            .set(Options.WORKER_IO_THREADS, 5)
            .set(Options.WORKER_TASK_CORE_THREADS, 3)
            .set(Options.WORKER_TASK_MAX_THREADS, 3)
            .set(Options.WORKER_NAME, "tcp-worker")
            .set(Options.KEEP_ALIVE, true);


    private Consumer<TcpConnection> onClose = NO_OP;
    private EventHandler handler;
    private final Set<Class> registeredTypes = new HashSet<>();
    private long keepAliveInterval = -1;
    private int bufferSize = Size.MB.ofInt(1);

    private TcpEventClient() {

    }

    public static TcpEventClient create() {
        return new TcpEventClient();
    }

    public <T> TcpEventClient option(Option<T> key, T value) {
        options.set(key, value);
        return this;
    }

    /**
     * Maximum event size
     */
    public TcpEventClient bufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
        return this;
    }

    public TcpEventClient onClose(Consumer<TcpConnection> onClose) {
        this.onClose = onClose;
        return this;
    }

    public TcpEventClient keepAlive(long timeout, TimeUnit unit) {
        this.keepAliveInterval = unit.toMillis(timeout);
        return this;
    }

    public TcpEventClient onEvent(EventHandler handler) {
        this.handler = handler;
        return this;
    }

    public TcpEventClient registerTypes(Class<?>... types) {
        registeredTypes.addAll(Arrays.asList(types));
        return this;
    }

    public TcpConnection connect(InetSocketAddress bindAddress, long timeout, TimeUnit unit) {
        XTcpClient client = new XTcpClient(options.getMap(), bindAddress, registeredTypes, bufferSize, keepAliveInterval, onClose, handler);
        return client.connect(timeout, unit);
    }
}
