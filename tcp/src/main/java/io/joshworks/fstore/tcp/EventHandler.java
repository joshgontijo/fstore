package io.joshworks.fstore.tcp;

public interface EventHandler {

    void onEvent(TcpConnection connection, Object data);
}
