package io.joshworks.fstore.ie.server;

import io.joshworks.fstore.core.io.buffers.BufferPool;
import io.joshworks.fstore.ie.server.protocol.Message;
import io.joshworks.fstore.tcp.TcpConnection;
import io.joshworks.ilog.record.RecordUtils;
import io.joshworks.ilog.index.RowKey;
import io.joshworks.ilog.lsm.Lsm;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Server implements Closeable {

    public static final AtomicLong sequence = new AtomicLong();
    private static final int DELETION = 1 << 2;
    private static final int APPEND = 1 << 3;
    private static final Logger log = LoggerFactory.getLogger(Server.class);
    private final Replicas replicas;
    //    private final TcpEventClient replicationClient;
    //    private final TcpEventServer tcpEndpoint;
    private final Lsm lsm;
    private final BufferPool bufferPool = BufferPool.defaultPool(10, 4096, false);

    public Server(File file, int... replicas) {
        this.lsm = Lsm.create(file, RowKey.LONG)
                .compactionThreshold(-1)
                .open();

        this.replicas = new Replicas(lsm);

        for (int replica : replicas) {
            this.replicas.addReplica(replica);
        }
    }

    public void append(ByteBuffer buffer, ReplicationLevel rlevel) {
        assert RecordUtils.isValid(buffer);

        long logId = lsm.append(buffer);
        sequence.set(logId);
        replicas.await(logId, rlevel);
    }

    private void onEvent(TcpConnection connection, Object data) {
//        var buffer = (ByteBuffer) data;
//        append(buffer);
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
        private final Lsm masterStore;
        private final Lock lock;
        private final Condition condition;

        public Replicas(Lsm masterStore) {
            this.masterStore = masterStore;
            this.lock = new ReentrantLock();
            this.condition = lock.newCondition();
        }

        public void addReplica(int port) {
            ReplicationWorker worker = new ReplicationWorker(port, this::onReplication, masterStore, -1, 8096 * 4, 0);
            workers.add(worker);
            worker.start();
        }

        public void await(long sequence, ReplicationLevel rlevel) {
            if (ReplicationLevel.LOCAL.equals(rlevel)) {
                return;
            }
            lock.lock();
            try {
                while (!replicated(sequence, rlevel)) {
                    //TODO make configurable
                    if (!condition.await(3, TimeUnit.SECONDS)) {
                        log.warn("Slow replication, sequence: {}", sequence);
                    }
                }
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                lock.unlock();
            }
        }

        private void onReplication(long sequence) {
            lock.lock();
            try {
                condition.signal();
            } finally {
                lock.unlock();
            }
        }

        private boolean replicated(long sequence, ReplicationLevel rlevel) {
            if (ReplicationLevel.LOCAL.equals(rlevel)) {
                return true;
            }
            int replicas = workers.size(); //TODO replace with quorum size
            int replications = 0;
            for (ReplicationWorker worker : workers) {
                if (worker.lasAcknowledgedSequence() >= sequence) {
                    replications++;
                }
            }
            switch (rlevel) {
                case ALL:
                    return replications >= replicas;
                case ONE:
                    return replications >= 1;
                case QUORUM:
                    return replications >= quorum();
            }

            throw new IllegalStateException("No valid replication level");
        }

        private int quorum() {
            //TODO wrong
            return (workers.size() / 2) + 1;
        }

        public void remove(TcpConnection conn) {
//            replicas.remove(conn);
        }

    }

}
