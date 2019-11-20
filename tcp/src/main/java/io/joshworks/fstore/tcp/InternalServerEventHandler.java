package io.joshworks.fstore.tcp;

import io.joshworks.fstore.tcp.internal.KeepAlive;
import io.joshworks.fstore.tcp.internal.Message;
import io.joshworks.fstore.tcp.internal.NullMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InternalServerEventHandler implements ServerEventHandler {

    private static final Logger logger = LoggerFactory.getLogger(InternalServerEventHandler.class);
    private final ServerEventHandler delegate;

    InternalServerEventHandler(ServerEventHandler delegate) {
        this.delegate = delegate;
    }

    @Override
    public void onEvent(TcpConnection connection, Object data) {
        if (data instanceof KeepAlive) {
            logger.debug("Received keep alive");
            System.out.println("KEEP ALIVE");
            return;
        }

        if (data instanceof Message) {
            Message msg = (Message) data;
            handleRequest(connection, msg);
            return;
        }

        delegate.onEvent(connection, data);
    }

    private void handleRequest(TcpConnection connection, Message message) {
        Object response = delegate.onRequest(connection, message.data);
        Object res = response == null ? new NullMessage() : response;
        connection.send(new Message(message.id, res));
    }

}
