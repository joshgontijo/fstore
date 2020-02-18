package io.joshworks.fstore.ie.server;

import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.ie.server.protocol.Message;
import io.joshworks.fstore.tcp.TcpConnection;
import io.joshworks.ilog.Record;
import io.joshworks.ilog.index.KeyComparator;
import io.joshworks.ilog.lsm.Lsm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class Server implements Closeable {

    private static final int DELETION = 1 << 2;
    private static final int APPEND = 1 << 3;

    private static final Logger log = LoggerFactory.getLogger(Server.class);

    private final Replicas replicas;

    //    private final TcpEventClient replicationClient;
    //    private final TcpEventServer tcpEndpoint;
    private final Lsm lsm;

    private final BufferPool bufferPool = BufferPool.defaultPool(10, 4096, false);

    public static final AtomicLong sequence = new AtomicLong();

    public Server(File file, int replicaPort) {
        this.lsm = Lsm.create(file, KeyComparator.LONG).open();
        this.replicas = new Replicas(lsm);
        replicas.addReplica(replicaPort);

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

    @Override
    public void close() {
        lsm.close();
    }

    private static class Replicas {
        private final Set<ReplicationWorker> workers = Collections.newSetFromMap(new ConcurrentHashMap<>());
        private final BlockingQueue<Long> q = new ArrayBlockingQueue<>(1);
        private final Lsm masterStore;

        public Replicas(Lsm masterStore) {
            this.masterStore = masterStore;
        }

        public void addReplica(int port) {
            ReplicationWorker worker = new ReplicationWorker(port, masterStore, -1, 8096 * 2, 50);
            workers.add(worker);
            worker.start();
        }

//        public void update(TcpConnection conn, long id) {
//            replicated.set(id);
//            long max = replicas.get(conn).accumulateAndGet(id, Math::max);
//            if (!q.offer(max)) {
//                q.poll();
//                q.offer(max);
//            }
//        }
//
//        private long replicated() {
//            long min = -1;
//            for (AtomicLong value : replicas.values()) {
//                min = Math.min(min, value.get());
//            }
//            return min;
//        }
//
//        public void remove(TcpConnection conn) {
//            replicas.remove(conn);
//        }
//
//        public void await(long id) {
//            try {
//                Long pooled;
//                do {
//                    pooled = q.poll(10, TimeUnit.SECONDS);
//                    if (pooled == null) {
//                        throw new RuntimeException("Replication timeout");
//                    }
//                } while (pooled < id);
//
//            } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//            }
//        }
    }

}
