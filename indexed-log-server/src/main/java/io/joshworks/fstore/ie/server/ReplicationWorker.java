package io.joshworks.fstore.ie.server;

import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.Threads;
import io.joshworks.fstore.ie.server.protocol.Replication;
import io.joshworks.fstore.tcp.TcpConnection;
import io.joshworks.ilog.LogIterator;
import io.joshworks.ilog.record.RecordUtils;
import io.joshworks.ilog.RecordBatch;
import io.joshworks.ilog.lsm.Lsm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;

class ReplicationWorker {


    private final SocketChannel sink;
    private final Lsm lsm;
    private final int poolMs;
    private final AtomicLong lastSentSequence = new AtomicLong();
    private final AtomicLong lasAckSequence = new AtomicLong();
    private final ByteBuffer buffer;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Thread thread;
    private static final Logger log = LoggerFactory.getLogger(ReplicationWorker.class);
    private final LongConsumer onReplication;

    ReplicationWorker(int replicaPort, LongConsumer onReplication, Lsm lsm, long lastSequence, int readSize, int poolMs) throws IOException {
        this.onReplication = onReplication;
        this.lsm = lsm;
        this.buffer = Buffers.allocate(readSize, false);
        this.poolMs = poolMs;
        this.lastSentSequence.set(Math.max(-1, lastSequence));
        this.sink = SocketChannel.open(new InetSocketAddress("localhost", replicaPort));

        this.thread = new Thread(() -> {
            try {
                this.replicate();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        this.receiver = new Thread(() -> {
            try {
                this.replicate();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void onReplicationEvent(TcpConnection connection, Object event) {
        var buffer = (ByteBuffer) event;
        try {
            if (Replication.messageType(buffer) == Replication.TYPE_LAST_REPLICATED) {
                long sequence = Replication.lastReplicatedId(buffer);
                log.info("Received ack from replica, sequence: {}", sequence);
                long acked = lasAckSequence.accumulateAndGet(sequence, Math::max);
                if (acked >= sequence) {
                    onReplication.accept(acked);
                }
            } else {
                throw new IllegalStateException("Invalid message type");
            }
        } finally {
            connection.pool().free(buffer);
        }
    }

    public void start() {
        log.info("Starting worker: {}", sink.peerAddress());
        thread.start();
    }

    public long lasAcknowledgedSequence() {
        return lasAckSequence.get();
    }

    public void close() {
        closed.set(true);
    }

    private void replicate() throws IOException {
//        Threads.sleep(20000);
        LogIterator it = lsm.logIterator();


        while (!closed.get()) {
            int read;
            do {
                read = it.read(buffer);
                if (read <= 0 && poolMs > 0) {
                    Threads.sleep(poolMs);
                }
            } while (read <= 0);

            buffer.flip();

            assert buffer.hasRemaining();

            int totalSize = 0;
            while (RecordBatch.hasNext(buffer)) {
                assert RecordUtils.isValid(buffer);

                long recordKey = buffer.getLong(buffer.position() + RecordUtils.KEY.offset(buffer));
                if (!lastSentSequence.compareAndSet(recordKey - 1, recordKey)) {
                    throw new IllegalStateException("Non contiguous record replication entry: " + recordKey + " current: " + lastSentSequence.get());
                }
                totalSize += RecordUtils.sizeOf(buffer);
                RecordBatch.advance(buffer);
            }

            assert totalSize > 0 : "When read > 0 then at least a record should be read";

            int plim = buffer.limit();
            buffer.position(0).limit(totalSize);
            sink.send(buffer, false);
            buffer.limit(plim); //restore, so we can compact
            buffer.compact();
        }
    }

}
