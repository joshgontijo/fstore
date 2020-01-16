package io.joshworks.fstore.tcp;

import io.joshworks.fstore.tcp.handlers.EventHandler;
import io.joshworks.fstore.tcp.internal.Message;
import io.joshworks.fstore.tcp.internal.Response;
import io.joshworks.fstore.tcp.internal.ResponseTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ResponseHandler implements EventHandler {

    private static final Logger logger = LoggerFactory.getLogger(ResponseHandler.class);

    private final EventHandler delegate;
    private final ResponseTable responseTable;

    ResponseHandler(EventHandler delegate, ResponseTable responseTable) {
        this.delegate = delegate;
        this.responseTable = responseTable;
    }

    @Override
    public void onEvent(TcpConnection connection, Object data) {
        if (!(data instanceof Message)) {
            delegate.onEvent(connection, data);
            return;
        }

        Message msg = (Message) data;
        Response<?> response = responseTable.remove(msg.id);
        if (response == null) {
            logger.warn("No response found for {}", msg.id);
            return;
        }
        response.complete(msg.data);
    }
}
