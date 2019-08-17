package io.joshworks.eventry.network.tcp_xnio.tcp;

public interface EventHandler  {

    void onEvent(TcpConnection connection, Object data);
}
