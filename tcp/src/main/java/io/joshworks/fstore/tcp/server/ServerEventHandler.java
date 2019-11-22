package io.joshworks.fstore.tcp.server;

import io.joshworks.fstore.tcp.TcpConnection;

@FunctionalInterface
public interface ServerEventHandler  {

    Object onEvent(TcpConnection connection, Object data);

}
