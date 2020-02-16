package io.joshworks.fstore.ie.server;

import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.core.util.Threads;
import io.joshworks.fstore.tcp.TcpConnection;
import io.joshworks.ilog.Record;
import io.joshworks.ilog.RecordBatch;
import io.joshworks.ilog.lsm.Lsm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

class ReplicationWorker {
    private final TcpConnection sink;
    private final Lsm src;
    private final int poolMs;
    private final AtomicLong sequence = new AtomicLong();
    private final ByteBuffer sinkBuffer;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Thread thread;

    private static final Logger log = LoggerFactory.getLogger(ReplicationWorker.class);

    ReplicationWorker(TcpConnection sink, Lsm src, long startSequence, int readSize, int poolMs) {
        assert startSequence >= 0;
        this.sink = sink;
        this.src = src;
        this.sinkBuffer = Buffers.allocate(Size.KB.ofInt(readSize), false);
        this.poolMs = poolMs;
        this.sequence.set(startSequence);
        this.thread = new Thread(this::replicate);
    }

    public void start() {
        log.info("Starting worker: {}", sink.peerAddress());
        thread.start();
    }

    public void close() {
        closed.set(true);
    }

    private void replicate() {
        while (!closed.get()) {

            sinkBuffer.clear();

            int read;
            do {
                read = src.readLog(sinkBuffer, sequence.get());
                if (read <= 0 && poolMs > 0) {
                    Threads.sleep(poolMs);
//                        log.info("No data to replicate...");
                }
            } while (read <= 0);

            sinkBuffer.flip();
            int plim = sinkBuffer.limit();

            while (RecordBatch.hasNext(sinkBuffer)) {
                long recordKey = sinkBuffer.getLong(sinkBuffer.position() + Record.KEY.offset(sinkBuffer));
                assert recordKey == sequence.get();
                int size = Record.sizeOf(sinkBuffer);
                Buffers.offsetLimit(sinkBuffer, size);
                sink.send(sinkBuffer, false);
                sinkBuffer.limit(plim);
                sequence.getAndIncrement();
            }
        }
    }
}
