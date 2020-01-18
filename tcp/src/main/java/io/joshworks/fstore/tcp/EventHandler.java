package io.joshworks.fstore.tcp;

@FunctionalInterface
public interface EventHandler {

    void onEvent(TcpConnection connection, Object data);
}
