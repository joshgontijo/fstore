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

class ReplicationWorker {
    private final TcpConnection sink;
    private final Lsm src;
    private final int poolMs;
    private final AtomicLong lastSentSequence = new AtomicLong();
    private final AtomicLong lasAckSequence = new AtomicLong();
    private final ByteBuffer sinkBuffer;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Thread thread;
    private static final Logger log = LoggerFactory.getLogger(ReplicationWorker.class);

    ReplicationWorker(int replicaPort, Lsm src, long startSequence, int readSize, int poolMs) {
        assert startSequence >= 0;
        this.src = src;
        this.sinkBuffer = Buffers.allocate(Size.KB.ofInt(readSize), false);
        this.poolMs = poolMs;
        this.lastSentSequence.set(startSequence);
        this.sink = TcpEventClient.create()
                .onEvent(this::onReplicationEvent)
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
                lasAckSequence.set(sequence);
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

    public long lastReplicated() {
        return lastSentSequence.get();
    }

    public void close() {
        closed.set(true);
    }

    private void replicate() {
        while (!closed.get()) {

            sinkBuffer.clear();

            int read;
            do {
                read = src.readLog(sinkBuffer, lastSentSequence.get());
                if (read <= 0 && poolMs > 0) {
                    Threads.sleep(poolMs);
//                        log.info("No data to replicate...");
                }
            } while (read <= 0);

            sinkBuffer.flip();
            int plim = sinkBuffer.limit();

            while (RecordBatch.hasNext(sinkBuffer)) {
                long recordKey = sinkBuffer.getLong(sinkBuffer.position() + Record.KEY.offset(sinkBuffer));
                assert recordKey == lastSentSequence.get() : recordKey;
                int size = Record.sizeOf(sinkBuffer);
                Buffers.offsetLimit(sinkBuffer, size);
                sink.send(sinkBuffer, false);
                sinkBuffer.limit(plim);
                lastSentSequence.getAndIncrement();
            }
        }
    }
}
