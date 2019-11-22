package io.joshworks.fstore.tcp.server;

import io.joshworks.fstore.tcp.EventHandler;
import io.joshworks.fstore.tcp.TcpConnection;

import java.util.concurrent.ExecutorService;

public class AsyncEventHandler implements EventHandler {

    private final ExecutorService executor;
    private final EventHandler next;

    public AsyncEventHandler(ExecutorService executor, EventHandler next) {
        this.executor = executor;
        this.next = next;
    }

    @Override
    public void onEvent(TcpConnection connection, Object data) {
        executor.execute(() -> next.onEvent(connection, data));
    }

}
