package io.joshworks.fstore.ie.server;

import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.ie.server.protocol.Replication;
import io.joshworks.fstore.tcp.TcpConnection;
import io.joshworks.fstore.tcp.TcpEventServer;
import io.joshworks.ilog.Record;
import io.joshworks.ilog.index.KeyComparator;
import io.joshworks.ilog.lsm.Lsm;
import org.jboss.threads.ArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xnio.Options;

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class Replica {

    private static final Logger log = LoggerFactory.getLogger(Replica.class);

    private final Lsm lsm;
    private final TcpEventServer receiver;
    private final ByteBuffer replicateBuffer = Buffers.allocate(8096, false);
    private final ByteBuffer protocolBuffer = Buffers.allocate(24, false);

    private final ReplicationExecutor writer = new ReplicationExecutor(100);

    static final AtomicLong sequence = new AtomicLong();


    public Replica(File dir, int port) {
        this.lsm = Lsm.create(dir, KeyComparator.LONG).open();
        this.receiver = TcpEventServer.create()
//                .idleTimeout(10, TimeUnit.SECONDS)
                .option(Options.WORKER_IO_THREADS, 1)
                .option(Options.WORKER_TASK_MAX_THREADS, 1)
                .option(Options.WORKER_TASK_CORE_THREADS, 1)
                .onEvent(this::handle)
                .start(new InetSocketAddress("localhost", port));

    }

    private void handle(TcpConnection connection, Object data) {
        writer.execute(connection, data, lsm, protocolBuffer, replicateBuffer);
    }

    public void close() {
        receiver.close();
    }

    private static final class ReplicationTask implements Runnable {

        private ByteBuffer buffer;
        private ByteBuffer replicateBuffer;
        private ByteBuffer protocolBuffer;
        private Lsm lsm;
        private TcpConnection connection;

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

                sequence.set(replId);

                protocolBuffer.clear();
                Replication.replicated(protocolBuffer, replId);
                protocolBuffer.flip();

//                log.info("[REPLICA] Replicated {}", logId);


//                connection.send(protocolBuffer, true);

            } finally {
                connection.pool().free(buffer);
            }
        }
    }

    private static class ReplicationExecutor extends ThreadPoolExecutor {

        private final Queue<ReplicationTask> runnables;

        public ReplicationExecutor(int size) {
            super(1, 1, 1, TimeUnit.HOURS, new BlockingExecutorQueue<>(size));
            this.runnables = new ArrayQueue<>(size + 10);
        }

        private void execute(TcpConnection connection, Object data, Lsm lsm, ByteBuffer protocolBuffer, ByteBuffer replicationBuffer) {
            ReplicationTask task = runnables.poll();
            task = task == null ? new ReplicationTask() : task;

            task.buffer = (ByteBuffer) data;
            task.protocolBuffer = protocolBuffer;
            task.replicateBuffer = replicationBuffer;
            task.connection = connection;
            task.lsm = lsm;

//            execute(task);
            task.run();
            runnables.offer(task);
        }

        @Override
        protected void afterExecute(Runnable r, Throwable t) {
            runnables.offer((ReplicationTask) r);
            super.afterExecute(r, t);
        }
    }

}
