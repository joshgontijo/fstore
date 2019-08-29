package io.joshworks.eventry.network.tcp.client;

import io.joshworks.eventry.network.tcp.EventHandler;
import io.joshworks.eventry.network.tcp.TcpConnection;
import io.joshworks.eventry.network.tcp.internal.Message;
import io.joshworks.eventry.network.tcp.internal.Response;
import io.joshworks.eventry.network.tcp.internal.ResponseTable;

import java.util.Map;

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
