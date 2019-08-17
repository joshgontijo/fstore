package io.joshworks.fstore.es.shared.tcp_xnio.tcp;

public interface EventHandler  {

    void onEvent(TcpConnection connection, Object data);
}
