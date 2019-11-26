package io.joshworks.lsm.server.handler;

import io.joshworks.fstore.tcp.TcpConnection;
import io.joshworks.fstore.tcp.server.TypedEventHandler;
import io.joshworks.lsm.server.messages.Ack;
import io.joshworks.lsm.server.messages.AssignReplica;
import io.joshworks.lsm.server.messages.CreateNamespace;
import io.joshworks.lsm.server.messages.Replicate;
import io.joshworks.lsm.server.replication.Replicas;

public class ReplicationHandler extends TypedEventHandler {

    private final Replicas replicas;

    public ReplicationHandler(Replicas replicas) {
        this.replicas = replicas;

        register(Replicate.class, this::replicate);
        register(AssignReplica.class, this::initialize);
        register(CreateNamespace.class, this::createNamespace);
    }

    @Override
    public Object onEvent(TcpConnection connection, Object data) {
        System.out.println("REPLICATION: " + data);
        return super.onEvent(connection, data);
    }

    private Ack createNamespace(TcpConnection connection, CreateNamespace msg) {

        return new Ack();
    }

    private Ack initialize(TcpConnection connection, AssignReplica msg) {
        replicas.initialize(msg.nodeId);
        return new Ack();
    }

    private Ack replicate(TcpConnection connection, Replicate msg) {
        replicas.replicate(msg);
        return new Ack();
    }

}
