package io.joshworks.fstore.ie.server;

import io.joshworks.fstore.ie.server.protocol.Replication;
import io.joshworks.fstore.tcp.TcpConnection;
import io.joshworks.ilog.Record;
import io.joshworks.ilog.lsm.Lsm;

import java.nio.ByteBuffer;

class ReplicationTask implements Runnable {

    ByteBuffer buffer;
    ByteBuffer replicateBuffer;
    ByteBuffer protocolBuffer;
    Lsm lsm;
    TcpConnection connection;

    @Override
    public void run() {
        try {
            int keyOffset = Record.KEY.offset(buffer);
            long replId = buffer.getLong(buffer.position() + keyOffset);

            replicateBuffer.clear();
            Record.VALUE.copyTo(buffer, replicateBuffer);
            replicateBuffer.flip();

            assert Record.isValid(replicateBuffer);
            long logId = lsm.append(replicateBuffer);

            assert logId == replId;

            Replica.sequence.set(replId);

            protocolBuffer.clear();
            Replication.replicated(protocolBuffer, replId);
            protocolBuffer.flip();

//                log.info("[REPLICA] Replicated {}", logId);


            connection.send(protocolBuffer, false);

        } finally {
            connection.pool().free(buffer);
        }
    }
}
