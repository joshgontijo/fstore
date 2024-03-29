package io.joshworks.fstore.ie.server;

import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.ie.server.protocol.Replication;
import io.joshworks.fstore.tcp.TcpConnection;
import io.joshworks.ilog.record.RecordBatch;
import io.joshworks.ilog.index.RowKey;
import io.joshworks.ilog.lsm.Lsm;
import org.jboss.threads.ArrayQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Queue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class Replica {

    private static final Logger log = LoggerFactory.getLogger(Replica.class);

    private final Lsm lsm;
    private final SocketChannel channel;
    private final ByteBuffer replicateBuffer = Buffers.allocate(8096, false);
    private final ByteBuffer protocolBuffer = Buffers.allocate(24, false);

    private final ReplicationExecutor writer = new ReplicationExecutor(100000);

    static final AtomicLong sequence = new AtomicLong();


    public Replica(File dir, int port) {
        this.lsm = Lsm.create(dir, RowKey.LONG).open();
        try {
            this.channel = SocketChannel.open(new InetSocketAddress("localhost", port));

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
//        this.receiver = TcpEventServer.create()
////                .idleTimeout(10, TimeUnit.SECONDS)
//                .name("replica-server")
//                .option(Options.WORKER_IO_THREADS, 1)
//                .option(Options.WORKER_TASK_MAX_THREADS, 1)
//                .option(Options.WORKER_TASK_CORE_THREADS, 1)
//                .option(Options.RECEIVE_BUFFER, Size.KB.ofInt(64))
//                .onOpen(conn -> System.out.println("Connection opened: " + conn))
//                .onEvent(this::handle)
//                .start(new InetSocketAddress("localhost", port));

    }

    public void run() {
        try {
            var buffer = ByteBuffer.allocate(4096 * 4);
            int read;
            while ((read = channel.read(buffer)) != -1) {
                if (read == 0) {
                    continue;
                }

                long lastSequence = -1;
                while (RecordBatch.hasNext(buffer)) {
                    int keyOffset = RecordUtils.KEY.offset(buffer);
                    long recordSequence = buffer.getLong(buffer.position() + keyOffset);

                    replicateBuffer.clear();
                    RecordUtils.VALUE.copyTo(buffer, replicateBuffer);
                    replicateBuffer.flip();

                    assert RecordUtils.isValid(replicateBuffer);
                    long logSequence = lsm.replicate(replicateBuffer);
                    assert logSequence == recordSequence : logSequence + " != " + recordSequence;
                    lastSequence = logSequence;
                    RecordBatch.advance(buffer);
                }

                if(lastSequence >= 0) {
                    sequence.set(lastSequence);
                    protocolBuffer.clear();
                    Replication.replicated(protocolBuffer, lastSequence);
                    protocolBuffer.flip();

                    channel.write(protocolBuffer);
                }
                buffer.compact();
            }

        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void handle(TcpConnection connection, Object data) {
        writer.execute(connection, data, lsm, protocolBuffer, replicateBuffer);
    }

    public void close() {
        try {
            channel.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static class ReplicationExecutor extends ThreadPoolExecutor {

        private final Queue<ReplicationReceiver> runnables;

        public ReplicationExecutor(int size) {
            super(1, 1, 1, TimeUnit.HOURS, new BlockingExecutorQueue<>(size));
            this.runnables = new ArrayQueue<>(size + 10);
        }

        private void execute(TcpConnection connection, Object data, Lsm lsm, ByteBuffer protocolBuffer, ByteBuffer replicationBuffer) {
            ReplicationReceiver task = runnables.poll();
            task = task == null ? new ReplicationReceiver() : task;

            task.buffer = (ByteBuffer) data;
            task.protocolBuffer = protocolBuffer;
            task.replicateBuffer = replicationBuffer;
            task.sc = connection;
            task.lsm = lsm;

//            execute(task);
            task.run();
            runnables.offer(task);
        }

        @Override
        protected void afterExecute(Runnable r, Throwable t) {
            runnables.offer((ReplicationReceiver) r);
            super.afterExecute(r, t);
        }
    }

}
