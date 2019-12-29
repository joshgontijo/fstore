package io.joshworks;

import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.appender.LogAppender;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.tcp.TcpMessageServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class ReplicationServer implements Closeable {

    private static final int BATCH_SIZE = 10;

    private static final Logger logger = LoggerFactory.getLogger(ReplicationServer.class);

    private final AtomicBoolean closed = new AtomicBoolean();
    private final TcpMessageServer server;
    private final LogAppender<Record> log;

    private final Map<String, LogIterator<Record>> iterators = new ConcurrentHashMap<>();
    private final Map<String, Long> lastReplicated = new ConcurrentHashMap<>();
    private final AtomicLong commitIndex = new AtomicLong(-1);


    public ReplicationServer(InetSocketAddress address, LogAppender<Record> log) {
        this.log = log;
        this.server = TcpMessageServer.create()
                .onOpen(conn -> {
                    //TODO if this node is not a master, then throws an exception
                    logger.info("Started replication server");
                })
//                .option(Options.WORKER_IO_THREADS, 1)
//                .option(Options.WORKER_TASK_CORE_THREADS, numReplicas)
                .rpcHandler(new ReplicationHandler())
                .start(address);

    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        server.close();
    }

    public void waitForReplication(WriteLevel writeLevel, long sequence) {
        if (WriteLevel.LOCAL.equals(writeLevel)) {
            return;
        }

        //TODO implement quorum

    }

    private class ReplicationHandler implements ReplicationRpc {

        @Override
        public void createIterator(String nodeId, long lastSequence) {
            logger.info("Creating iterator for node {}, from sequence {}", nodeId, lastSequence);
            long startPos = Log.START;
            if (lastSequence >= 0) { //replica has some data, find the start point
                try (LogIterator<Record> iterator = log.iterator(Direction.BACKWARD)) {
                    while (iterator.hasNext()) {
                        startPos = iterator.position();
                        Record next = iterator.next();
                        if (next.sequence == lastSequence) {
                            break;
                        }
                    }
                }
            }
            LogIterator<Record> iterator = log.iterator(Direction.FORWARD, startPos);

            iterators.put(nodeId, iterator);
            lastReplicated.put(nodeId, lastSequence);
        }

        @Override
        public List<Record> fetch(String nodeId, long lastSequence) {
            LogIterator<Record> iterator = iteratorFor(nodeId);

            lastReplicated.put(nodeId, lastSequence);

            List<Record> entries = new ArrayList<>(BATCH_SIZE);
            while (entries.size() < BATCH_SIZE && iterator.hasNext()) {
                Record next = iterator.next();
                if (next.sequence < lastSequence) {
                    throw new RuntimeException("Non sequential replication entry");
                }
                entries.add(next);
            }
            return entries;
        }

        private LogIterator<Record> iteratorFor(String nodeId) {
            LogIterator<Record> iterator = iterators.get(nodeId);
            if (iterator == null) {
                throw new RuntimeException("No iterator for nodeId " + nodeId);
            }
            return iterator;
        }

        @Override
        public long position(String nodeId) {
            return iteratorFor(nodeId).position();
        }
    }

}
