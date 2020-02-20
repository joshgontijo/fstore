package io.joshworks.fstore.ie.server;

import io.joshworks.fstore.ie.server.protocol.Replication;
import io.joshworks.fstore.tcp.TcpConnection;
import io.joshworks.ilog.Record;
import io.joshworks.ilog.RecordBatch;
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

            long lastSequence = -1;
            while (RecordBatch.hasNext(buffer)) {
                int keyOffset = Record.KEY.offset(buffer);
                long recordSequence = buffer.getLong(buffer.position() + keyOffset);

                replicateBuffer.clear();
                Record.VALUE.copyTo(buffer, replicateBuffer);
                replicateBuffer.flip();

                assert Record.isValid(replicateBuffer);
                long logSequence = lsm.append(replicateBuffer);
                assert logSequence == recordSequence;
                lastSequence = logSequence;
                RecordBatch.advance(buffer);
            }

            Replica.sequence.set(lastSequence);

            protocolBuffer.clear();
            Replication.replicated(protocolBuffer, lastSequence);
            protocolBuffer.flip();

            connection.send(protocolBuffer, false);

        } finally {
            connection.pool().free(buffer);
        }
    }
}
