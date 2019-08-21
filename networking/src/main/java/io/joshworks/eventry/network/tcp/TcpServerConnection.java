package io.joshworks.eventry.network.tcp;

import io.joshworks.fstore.core.io.buffers.BufferPool;
import org.xnio.StreamConnection;

import java.nio.ByteBuffer;
import java.util.Map;

public class TcpServerConnection extends TcpConnection {
    private final Map<StreamConnection, TcpConnection> connections;

    TcpServerConnection(StreamConnection connection, Map<StreamConnection, TcpConnection> connections, BufferPool bufferPool) {
        super(connection, bufferPool);
        this.connections = connections;
    }

    public void broadcast(ByteBuffer data) {
        for (TcpConnection tcpc : connections.values()) {
            tcpc.send(data.slice());
        }
    }
}
