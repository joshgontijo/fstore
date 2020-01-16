package io.joshworks.fstore.tcp.server;

import io.joshworks.fstore.tcp.handlers.EventHandler;
import io.joshworks.fstore.tcp.TcpConnection;
import io.joshworks.fstore.tcp.internal.KeepAlive;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

public class KeepAliveHandler implements EventHandler {

    private static final Logger logger = LoggerFactory.getLogger(KeepAliveHandler.class);

    private final EventHandler next;
    private final ByteBuffer DATA_WRAP = ByteBuffer.wrap(KeepAlive.DATA);

    public KeepAliveHandler(EventHandler next) {
        this.next = next;
    }

    @Override
    public void onEvent(TcpConnection connection, Object data) {
        if (DATA_WRAP.equals(data)) {
            logger.debug("Received keep alive");
            return;
        }
        next.onEvent(connection, data);
    }
}
