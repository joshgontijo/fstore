package io.joshworks.lsm.server.handler;

import io.joshworks.fstore.tcp.server.ServerEventHandler;
import io.joshworks.fstore.tcp.TcpConnection;
import io.joshworks.lsm.server.LsmCluster;
import io.joshworks.lsm.server.messages.Ack;
import io.joshworks.lsm.server.messages.Delete;
import io.joshworks.lsm.server.messages.Get;
import io.joshworks.lsm.server.messages.Put;
import io.joshworks.lsm.server.messages.Result;

public class TcpEventHandler implements ServerEventHandler {


    private final LsmCluster lsmtree;

    public TcpEventHandler(LsmCluster lsmtree) {
        this.lsmtree = lsmtree;

        handlers.add(Put.class, this::put);
        handlers.add(Get.class, this::get);
        handlers.add(Delete.class, this::delete);
//        handlers.add(CreateNamespace.class, this::createNamespace);
    }

    @Override
    public Object onRequest(TcpConnection connection, Object data) {
        return handlers.handle(data, connection);
    }

    @Override
    public void onEvent(TcpConnection connection, Object data) {
        handlers.handle(data, connection);
    }

//    private Ack createNamespace(TcpConnection connection, CreateNamespace msg) {
//
//    }

    private Ack put(TcpConnection connection, Put msg) {
        lsmtree.put(msg);
        return new Ack();
    }

    private Result get(TcpConnection connection, Get msg) {
        return lsmtree.get(msg);
    }

    private Ack delete(TcpConnection connection, Delete msg) {
        lsmtree.delete(msg);
        return new Ack();
    }


}
