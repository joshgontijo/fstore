package io.joshworks.fstore.tcp.server;

import io.joshworks.fstore.tcp.TcpConnection;

public class DiscardEventHandler implements ServerEventHandler {

    @Override
    public Object onEvent(TcpConnection connection, Object data) {
        return null;
    }
}
