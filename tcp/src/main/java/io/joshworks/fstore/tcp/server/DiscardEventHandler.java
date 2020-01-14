package io.joshworks.fstore.tcp.server;

import io.joshworks.fstore.tcp.EventHandler;
import io.joshworks.fstore.tcp.TcpConnection;

public class DiscardEventHandler implements EventHandler {

    @Override
    public void onEvent(TcpConnection connection, Object data) {

    }
}
