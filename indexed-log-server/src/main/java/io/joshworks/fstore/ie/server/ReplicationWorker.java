package io.joshworks.fstore.ie.server;

import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.core.util.Threads;
import io.joshworks.fstore.ie.server.protocol.Replication;
import io.joshworks.fstore.tcp.TcpConnection;
import io.joshworks.fstore.tcp.TcpEventClient;
import io.joshworks.ilog.Record;
import io.joshworks.ilog.RecordBatch;
import io.joshworks.ilog.lsm.Lsm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xnio.Options;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;

class ReplicationWorker {


    private final TcpConnection sink;
    private final Lsm src;
    private final int poolMs;
    private final AtomicLong lastSentSequence = new AtomicLong();
    private final AtomicLong lasAckSequence = new AtomicLong();
    private final ByteBuffer buffer;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Thread thread;
    private static final Logger log = LoggerFactory.getLogger(ReplicationWorker.class);
    private final LongConsumer onReplication;

    ReplicationWorker(int replicaPort, LongConsumer onReplication, Lsm src, long lastSequence, int readSize, int poolMs) {
        this.onReplication = onReplication;
        this.src = src;
        this.buffer = Buffers.allocate(readSize, false);
        this.poolMs = poolMs;
        this.lastSentSequence.set(Math.max(-1, lastSequence));
        this.sink = TcpEventClient.create()
                .name("replication-worker")
                .maxMessageSize(readSize)
                .onEvent(this::onReplicationEvent)
                .option(Options.SEND_BUFFER, Size.KB.ofInt(16))
                .option(Options.WORKER_IO_THREADS, 1)
                .option(Options.WORKER_TASK_CORE_THREADS, 1)
                .option(Options.WORKER_TASK_MAX_THREADS, 1)
                .connect(new InetSocketAddress("localhost", replicaPort), 5, TimeUnit.SECONDS);

        this.thread = new Thread(this::replicate);
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

    private void replicate() {
        while (!closed.get()) {
            buffer.clear();
            int read;
            do {
                read = src.readLog(buffer, lastSentSequence.get() + 1);
                if (read <= 0) {
                    if (poolMs > 0) {
                        Threads.sleep(poolMs);
                    }
                }
            } while (read <= 0);

            buffer.flip();

            int totalSize = 0;
            while (RecordBatch.hasNext(buffer)) {
                long recordKey = buffer.getLong(buffer.position() + Record.KEY.offset(buffer));
                if (!lastSentSequence.compareAndSet(recordKey - 1, recordKey)) {
                    throw new IllegalStateException("Non contiguous record replication entry: " + recordKey + " current: " + lastSentSequence.get());
                }
                assert Record.isValid(buffer);

                totalSize += Record.sizeOf(buffer);
                RecordBatch.advance(buffer);
            }
            buffer.position(0).limit(totalSize);
            sink.send(buffer, false);
        }
    }

}
