package io.joshworks.fstore.ie.server;

import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.ie.server.protocol.Replication;
import io.joshworks.fstore.tcp.TcpConnection;
import io.joshworks.fstore.tcp.TcpEventClient;
import io.joshworks.ilog.Record;
import io.joshworks.ilog.index.KeyComparator;
import io.joshworks.ilog.lsm.Lsm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xnio.Options;

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Replica {

    private static final Logger log = LoggerFactory.getLogger(Replica.class);

    private final Lsm lsm;
    private final TcpConnection client;

    private final ByteBuffer replicateBuffer = Buffers.allocate(8096, false);
    private final ByteBuffer protocolBuffer = Buffers.allocate(24, false);

    private final ReplicationExecutor writer = new ReplicationExecutor(100);

    public Replica(File dir, int port) {
        this.lsm = Lsm.create(dir, KeyComparator.LONG).open();
        this.client = TcpEventClient.create()
                .onEvent(this::handle)
                .option(Options.WORKER_IO_THREADS, 1)
                .option(Options.WORKER_TASK_CORE_THREADS, 1)
                .option(Options.WORKER_TASK_MAX_THREADS, 1)
                .connect(new InetSocketAddress("localhost", port), 5, TimeUnit.SECONDS);

    }

    private void handle(TcpConnection connection, Object data) {
        writer.execute(connection, data, lsm, protocolBuffer, replicateBuffer);
    }

    public void close() {
        client.close();
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

                protocolBuffer.clear();
                Replication.replicated(protocolBuffer, replId);
                protocolBuffer.flip();

                log.info("[REPLICA] Replicated {}", logId);

                connection.send(protocolBuffer, true);

            } finally {
                connection.pool().free(buffer);
            }
        }
    }

    private static class ReplicationExecutor extends ThreadPoolExecutor {

        private final Queue<ReplicationTask> runnables;

        public ReplicationExecutor(int size) {
            super(1, 1, 1, TimeUnit.HOURS, new BlockingExecutorQueue<>(size));
            this.runnables = new LinkedList<>();
        }

        private void execute(TcpConnection connection, Object data, Lsm lsm, ByteBuffer protocolBuffer, ByteBuffer replicationBuffer) {
            ReplicationTask task = runnables.poll();
            task = task == null ? new ReplicationTask() : task;

            task.buffer = (ByteBuffer) data;
            task.protocolBuffer = protocolBuffer;
            task.replicateBuffer = replicationBuffer;
            task.connection = connection;
            task.lsm = lsm;

            execute(task);

        }

        @Override
        protected void afterExecute(Runnable r, Throwable t) {
            runnables.offer((ReplicationTask) r);
            super.afterExecute(r, t);
        }
    }

}
