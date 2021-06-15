package io.joshworks.fstore.ie.server;

import io.joshworks.fstore.tcp.EventHandler;
import io.joshworks.fstore.tcp.TcpConnection;
import io.joshworks.ilog.lsm.Lsm;

import java.nio.ByteBuffer;

public class TcpEventHandler implements EventHandler {

    private final Lsm lsm;

    public TcpEventHandler(Lsm lsm) {
        this.lsm = lsm;
    }

    @Override
    public void onEvent(TcpConnection connection, Object data) {
        var buffer = (ByteBuffer) data;


    }
}
