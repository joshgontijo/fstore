package io.joshworks.fstore.tcp.client;

import io.joshworks.fstore.tcp.EventHandler;
import io.joshworks.fstore.tcp.TcpConnection;
import io.joshworks.fstore.tcp.internal.Message;
import io.joshworks.fstore.tcp.internal.Response;
import io.joshworks.fstore.tcp.internal.ResponseTable;

public class InternalClientEventHandler implements EventHandler {

    private final EventHandler delegate;
    private final ResponseTable responseTable;

    public InternalClientEventHandler(EventHandler delegate, ResponseTable responseTable) {
        this.delegate = delegate;
        this.responseTable = responseTable;
    }

    @Override
    public void onEvent(TcpConnection connection, Object data) {
        if (data instanceof Message) {
            Message msg = (Message) data;
            Response response = responseTable.complete(msg.id);
            if (response == null) {
                //TODO log and discard ?
                System.err.println("Received null from the server");
                throw new RuntimeException("No response correlated for request " + msg.id);
            }
            response.complete(msg.data);
            return;
        }

        delegate.onEvent(connection, data);

    }
}
