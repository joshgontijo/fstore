package io.joshworks.eventry.network.tcp;

public interface EventHandler {

    void onEvent(TcpConnection connection, Object data);
}
