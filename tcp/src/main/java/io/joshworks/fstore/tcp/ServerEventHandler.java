package io.joshworks.fstore.tcp;

public interface ServerEventHandler extends EventHandler {

    default Object onRequest(TcpConnection connection, Object data) {
        //do nothing
        return null;
    }

}
