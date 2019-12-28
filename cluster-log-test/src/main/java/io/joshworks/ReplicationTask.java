package io.joshworks;

import io.joshworks.fstore.log.appender.LogAppender;
import io.joshworks.fstore.tcp.TcpClientConnection;
import io.joshworks.fstore.tcp.client.TcpEventClient;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class ReplicationTask<T> implements Runnable, Closeable {

    private final AtomicBoolean closed = new AtomicBoolean();
    private final TcpClientConnection connection;
    private final ReplicationRpc proxy;
    private final int interval;

    public ReplicationTask(InetSocketAddress address, int interval, int replicationThreshold, LogAppender<Entry<T>> log) {
        this.connection = TcpEventClient.create().connect(address, 5, TimeUnit.SECONDS);
        this.proxy = connection.createRpcProxy(ReplicationRpc.class, 10_000);
        this.interval = interval;
    }

    @Override
    public void run() {
        while(!closed.get()) {
            proxy.

            //sleep

        }
    }

    @Override
    public void close() {
        if(!closed.compareAndSet(false, true)) {
            return;
        }
    }
}
