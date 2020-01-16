package io.joshworks.fstore.tcp.handlers;

import io.joshworks.fstore.tcp.TcpConnection;

@FunctionalInterface
public interface EventHandler {

    void onEvent(TcpConnection connection, Object data);
}
