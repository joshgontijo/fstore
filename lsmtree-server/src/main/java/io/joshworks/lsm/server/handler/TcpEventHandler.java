package io.joshworks.lsm.server.handler;

import io.joshworks.fstore.tcp.TcpConnection;
import io.joshworks.fstore.tcp.server.TypedEventHandler;
import io.joshworks.lsm.server.LsmCluster;
import io.joshworks.lsm.server.messages.Ack;
import io.joshworks.lsm.server.messages.CreateNamespace;
import io.joshworks.lsm.server.messages.Delete;
import io.joshworks.lsm.server.messages.Get;
import io.joshworks.lsm.server.messages.Put;
import io.joshworks.lsm.server.messages.Result;

public class TcpEventHandler extends TypedEventHandler {

    private final LsmCluster lsmtree;

    public TcpEventHandler(LsmCluster lsmtree) {
        this.lsmtree = lsmtree;

        register(Put.class, this::put);
        register(Get.class, this::get);
        register(Delete.class, this::delete);
        register(CreateNamespace.class, this::createNamespace);
    }

    @Override
    public void onEvent(TcpConnection connection, Object data) {
        System.out.println("RECEIVED: " + data);
        super.onEvent(connection, data);
    }

    private Ack createNamespace(TcpConnection connection, CreateNamespace msg) {
        return new Ack();
    }

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
