package io.joshworks.fstore.ie.server;

import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.core.util.Threads;
import io.joshworks.fstore.ie.server.protocol.Replication;
import io.joshworks.fstore.tcp.BatchWriter;
import io.joshworks.fstore.tcp.TcpConnection;
import io.joshworks.fstore.tcp.TcpEventClient;
import io.joshworks.fstore.tcp.codec.Compression;
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

    public static final long NONE = -1;

    private final TcpConnection sink;
    private final Lsm src;
    private final int poolMs;
    private final AtomicLong lastSentSequence = new AtomicLong();
    private final AtomicLong lasAckSequence = new AtomicLong();
    private final ByteBuffer buffer;
    private final BatchWriter writer;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Thread thread;
    private static final Logger log = LoggerFactory.getLogger(ReplicationWorker.class);

    ReplicationWorker(int replicaPort, Lsm src, long startSequence, int readSize, int poolMs) {
        assert startSequence >= NONE;
        this.src = src;
        this.buffer = Buffers.allocate(readSize, false);
        this.poolMs = poolMs;
        this.lastSentSequence.set(startSequence);
        this.sink = TcpEventClient.create()
                .compression(Compression.LZ4_FAST)
                .onEvent(this::onReplicationEvent)
                .option(Options.SEND_BUFFER, Size.KB.ofInt(16))
                .option(Options.WORKER_IO_THREADS, 1)
                .option(Options.WORKER_TASK_CORE_THREADS, 1)
                .option(Options.WORKER_TASK_MAX_THREADS, 1)
                .connect(new InetSocketAddress("localhost", replicaPort), 5, TimeUnit.SECONDS);

        this.writer = sink.batching(readSize);
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

            buffer.clear();

            int read;
            do {
                read = src.readLog(buffer, lastSentSequence.get() + 1);
                if (read <= 0 && poolMs > 0) {
                    writer.flush(false);
                    Threads.sleep(poolMs);
//                        log.info("No data to replicate...");
                }
            } while (read <= 0);

            buffer.flip();
            int plim = buffer.limit();

            buffer.clear();

            while (RecordBatch.hasNext(buffer)) {
                long recordKey = buffer.getLong(buffer.position() + Record.KEY.offset(buffer));
                if (!lastSentSequence.compareAndSet(recordKey - 1, recordKey)) {
                    throw new IllegalStateException("Non contiguous record replication entry");
                }

                int size = Record.sizeOf(buffer);
                Buffers.offsetLimit(buffer, size);

                assert Record.isValid(buffer);

                writer.write(buffer);
                buffer.limit(plim);
            }
        }
    }
}
