package io.joshworks.fstore.ie.server;

import io.joshworks.fstore.ie.server.protocol.Replication;
import io.joshworks.fstore.tcp.TcpHeader;
import io.joshworks.ilog.Record;
import io.joshworks.ilog.RecordBatch;
import io.joshworks.ilog.lsm.Lsm;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

class ReplicationReceiver implements Runnable {

    ByteBuffer buffer = ByteBuffer.allocate(4096 * 4);
    ByteBuffer replicateBuffer;
    ByteBuffer protocolBuffer;
    Lsm lsm;
    SocketChannel sc;

    public ReplicationReceiver(int port) throws IOException {
        this.sc = SocketChannel.open(new InetSocketAddress("localhost", port));
    }

    @Override
    public void run() {
        try {
            int read;
            while ((read = sc.read(buffer)) != -1) {
                if (read == 0) {
                    continue;
                }

                long lastSequence = -1;
                while (RecordBatch.hasNext(buffer)) {
                    int keyOffset = Record.KEY.offset(buffer);
                    long recordSequence = buffer.getLong(buffer.position() + keyOffset);

                    replicateBuffer.clear();
                    Record.VALUE.copyTo(buffer, replicateBuffer);
                    replicateBuffer.flip();

                    assert Record.isValid(replicateBuffer);
                    long logSequence = lsm.append(replicateBuffer);
                    assert logSequence == recordSequence : logSequence + " != " + recordSequence;
                    lastSequence = logSequence;
                    RecordBatch.advance(buffer);
                }

                Replica.sequence.set(lastSequence);

                protocolBuffer.clear();
                Replication.replicated(protocolBuffer, lastSequence);
                protocolBuffer.flip();

                sc.write(protocolBuffer);
                buffer.compact();
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
