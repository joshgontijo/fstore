package io.joshworks.lsm.server.handler;

import io.joshworks.fstore.tcp.ServerEventHandler;
import io.joshworks.fstore.tcp.TcpConnection;
import io.joshworks.lsm.server.LsmCluster;

public class ReplicationHandler implements ServerEventHandler {

    private final LsmCluster lsmtree;

    public ReplicationHandler(LsmCluster lsmtree) {
        this.lsmtree = lsmtree;
    }

    @Override
    public void onEvent(TcpConnection connection, Object data) {

    }

    @Override
    public Object onRequest(TcpConnection connection, Object data) {
        return null;
    }
}
