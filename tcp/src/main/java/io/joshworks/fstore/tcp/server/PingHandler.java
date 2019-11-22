package io.joshworks.fstore.tcp.server;

import io.joshworks.fstore.tcp.TcpConnection;
import io.joshworks.fstore.tcp.internal.Ping;
import io.joshworks.fstore.tcp.internal.Pong;

public class PingHandler implements ServerEventHandler {

    private final ServerEventHandler next;

    public PingHandler(ServerEventHandler next) {
        this.next = next;
    }

    @Override
    public Object onEvent(TcpConnection connection, Object data) {
        if (data instanceof Ping) {
            Ping ping = (Ping) data;
            return new Pong(System.currentTimeMillis() - ping.timestamp);
        }
        return next.onEvent(connection, data);
    }
}
