package io.joshworks.fstore.tcp.client;

import io.joshworks.fstore.tcp.EventHandler;
import io.joshworks.fstore.tcp.TcpConnection;
import io.joshworks.fstore.tcp.internal.Message;
import io.joshworks.fstore.tcp.internal.Response;
import io.joshworks.fstore.tcp.internal.ResponseTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientResponseHandler implements EventHandler {

    private static final Logger logger = LoggerFactory.getLogger(ClientResponseHandler.class);

    private final EventHandler delegate;
    private final ResponseTable responseTable;

    public ClientResponseHandler(EventHandler delegate, ResponseTable responseTable) {
        this.delegate = delegate;
        this.responseTable = responseTable;
    }

    @Override
    public void onEvent(TcpConnection connection, Object data) {
        if (data instanceof Message) {
            Message msg = (Message) data;
            Response response = responseTable.complete(msg.id);
            if (response == null) {
                logger.warn("No response found for {}", msg.id);
                return;
            }
            response.complete(msg.data);
            return;
        }

        delegate.onEvent(connection, data);

    }
}
