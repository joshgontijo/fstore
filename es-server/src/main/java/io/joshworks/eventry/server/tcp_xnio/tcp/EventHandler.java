package io.joshworks.eventry.server.tcp_xnio.tcp;

public interface EventHandler  {

    void onEvent(TcpConnection connection, Object data);
}
