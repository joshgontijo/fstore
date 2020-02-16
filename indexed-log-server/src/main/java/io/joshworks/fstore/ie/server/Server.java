package io.joshworks.fstore.ie.server;

import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.ie.server.protocol.Message;
import io.joshworks.fstore.ie.server.protocol.Replication;
import io.joshworks.fstore.tcp.TcpConnection;
import io.joshworks.fstore.tcp.TcpEventServer;
import io.joshworks.fstore.tcp.codec.Compression;
import io.joshworks.ilog.Record;
import io.joshworks.ilog.index.KeyComparator;
import io.joshworks.ilog.lsm.Lsm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xnio.Options;

import java.io.Closeable;
import java.io.File;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class Server implements Closeable {

    private static final int DELETION = 1 << 2;
    private static final int APPEND = 1 << 3;

    private static final Logger log = LoggerFactory.getLogger(Server.class);

    private final Replicas replicas = new Replicas();

    private final TcpEventServer replicationServer;
    //    private final TcpEventServer tcpEndpoint;
    private final Lsm lsm;

    private final BufferPool bufferPool = BufferPool.defaultPool(10, 4096, false);

    public static final AtomicLong sequence = new AtomicLong();
    public static final AtomicLong replicated = new AtomicLong();

    public Server(File file, int replicationPort) {
        this.lsm = Lsm.create(file, KeyComparator.LONG).open();
        this.replicationServer = TcpEventServer.create()
                .compression(Compression.SNAPPY)
                .idleTimeout(10, TimeUnit.SECONDS)
                .option(Options.WORKER_IO_THREADS, 1)
                .option(Options.WORKER_TASK_MAX_THREADS, 1)
                .option(Options.WORKER_TASK_CORE_THREADS, 1)
                .onOpen(conn -> replicas.init(conn, lsm))
                .onEvent(this::onReplicationEvent)
                .onClose(replicas::remove)
                .start(new InetSocketAddress("localhost", replicationPort));


//        this.tcpEndpoint = TcpEventServer.create()
//                .idleTimeout(10, TimeUnit.SECONDS)
//                .option(Options.WORKER_IO_THREADS, 1)
//                .option(Options.WORKER_TASK_MAX_THREADS, 1)
//                .option(Options.WORKER_TASK_CORE_THREADS, 1)
//                .onEvent(this::onEvent)
//                .start(new InetSocketAddress("localhost", publicPort));

    }

    private void onReplicationEvent(TcpConnection connection, Object event) {
        var buffer = (ByteBuffer) event;
        try {
            if (Replication.messageType(buffer) == Replication.TYPE_LAST_REPLICATED) {
                long id = Replication.lastReplicatedId(buffer);
                log.info("Received ack from replica, sequence: {}", id);
                replicas.update(connection, id);
            } else {
                throw new IllegalStateException("Invalid message type");
            }

        } finally {
            connection.pool().free(buffer);
        }
    }

    public void append(ByteBuffer buffer) {
        assert Record.isValid(buffer);

        long logId = lsm.append(buffer);
        sequence.set(logId);
//        replicas.await(logId);

//        ackBack(connection);
    }

    private void onEvent(TcpConnection connection, Object data) {
        var buffer = (ByteBuffer) data;
        append(buffer);
    }


    private void ackBack(TcpConnection connection) {
        var respBuffer = bufferPool.allocate();
        try {
            Message.writeAck(respBuffer);
            respBuffer.flip();
            connection.send(respBuffer, false);
        } finally {
            bufferPool.free(respBuffer);
        }
    }

    public void awaitTermination() throws InterruptedException {
        replicationServer.awaitTermination();
    }

    @Override
    public void close() {
        replicationServer.close();
        lsm.close();
    }

    private static class Replicas {
        private final Map<TcpConnection, AtomicLong> replicas = new ConcurrentHashMap<>();
        private final Map<TcpConnection, ReplicationWorker> workers = new ConcurrentHashMap<>();
        private final BlockingQueue<Long> q = new ArrayBlockingQueue<>(1);

        public void init(TcpConnection conn, Lsm src) {
            replicas.put(conn, new AtomicLong());
            ReplicationWorker worker = new ReplicationWorker(conn, src, 0, 8096, -1);
            workers.put(conn, worker);
            worker.start();

        }

        public void update(TcpConnection conn, long id) {
            replicated.set(id);
            long max = replicas.get(conn).accumulateAndGet(id, Math::max);
            if (!q.offer(max)) {
                q.poll();
                q.offer(max);
            }
        }

        private long replicated() {
            long min = -1;
            for (AtomicLong value : replicas.values()) {
                min = Math.min(min, value.get());
            }
            return min;
        }

        public void remove(TcpConnection conn) {
            replicas.remove(conn);
        }

        public void await(long id) {
            try {
                Long pooled;
                do {
                    pooled = q.poll(10, TimeUnit.SECONDS);
                    if (pooled == null) {
                        throw new RuntimeException("Replication timeout");
                    }
                } while (pooled < id);

            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
