package io.joshworks.fstore.tcp.server;

import io.joshworks.fstore.tcp.EventHandler;
import io.joshworks.fstore.tcp.TcpConnection;
import io.joshworks.fstore.tcp.internal.KeepAlive;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KeepAliveHandler implements EventHandler {

    private static final Logger logger = LoggerFactory.getLogger(KeepAliveHandler.class);

    private final EventHandler next;

    public KeepAliveHandler(EventHandler next) {
        this.next = next;
    }

    @Override
    public void onEvent(TcpConnection connection, Object data) {
        if (data instanceof KeepAlive) {
            logger.debug("Received keep alive");
            return;
        }
        next.onEvent(connection, data);
    }
}
