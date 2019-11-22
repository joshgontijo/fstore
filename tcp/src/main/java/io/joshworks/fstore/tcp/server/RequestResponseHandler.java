package io.joshworks.fstore.tcp.server;

import io.joshworks.fstore.tcp.EventHandler;
import io.joshworks.fstore.tcp.TcpConnection;
import io.joshworks.fstore.tcp.internal.Message;
import io.joshworks.fstore.tcp.internal.NullMessage;

public class RequestResponseHandler implements EventHandler {

    private final ServerEventHandler next;

    public RequestResponseHandler(ServerEventHandler next) {
        this.next = next;
    }

    @Override
    public void onEvent(TcpConnection connection, Object data) {
        if (data instanceof Message) {
            Message msg = (Message) data;
            Object response = next.onEvent(connection, msg.data);
            Object res = response == null ? new NullMessage() : response;
            connection.send(new Message(msg.id, res));
            return;
        }

        next.onEvent(connection, data);

    }
}
