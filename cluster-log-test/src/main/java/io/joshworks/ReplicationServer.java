package io.joshworks;

import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.appender.LogAppender;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.tcp.TcpMessageServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xnio.Options;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

public class ReplicationServer implements Closeable {

    private static final int BATCH_SIZE = 10;

    private static final Logger logger = LoggerFactory.getLogger(ReplicationServer.class);

    private final AtomicBoolean closed = new AtomicBoolean();
    private final TcpMessageServer server;
    private final int clusterSize;
    private final CommitTable commitTable;
    private final LogAppender<Record> log;

    private final Map<String, LastEntry> tracker = new ConcurrentHashMap<>();

    private final BlockingQueue<ReplicationSignal> writeItems = new ArrayBlockingQueue<>(1000);

    public ReplicationServer(InetSocketAddress address, LogAppender<Record> log, int clusterSize, CommitTable commitTable) {
        this.log = log;
        this.clusterSize = clusterSize;
        this.commitTable = commitTable;
        this.server = TcpMessageServer.create()
                .onOpen(conn -> {
                    //TODO if this node is not a master, then throws an exception
                    logger.info("Started replication server");
                })
                .option(Options.WORKER_IO_THREADS, 1)
                .option(Options.WORKER_TASK_CORE_THREADS, 3)
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

    public WorkItem<Long> waitForReplication(ReplicationLevel writeLevel, long sequence) {
        WorkItem<Long> workItem = new WorkItem<>(sequence);
        if (ReplicationLevel.LOCAL.equals(writeLevel)) {
            workItem.complete(sequence);
            return workItem;
        }
        ReplicationSignal signal = new ReplicationSignal(workItem, writeLevel, sequence, clusterSize);
        if (!writeItems.offer(signal)) {
            //write queue is full
            try {
                writeItems.put(signal);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
        return workItem;

    }

    private class ReplicationHandler implements ReplicationRpc {

        @Override
        public void createIterator(String nodeId, Long lastSequence) {
            long headPos = Log.START;
            if (lastSequence >= 0) { //replica has some data, find the start point
                try (LogIterator<Record> iterator = log.iterator(Direction.BACKWARD)) {
                    while (iterator.hasNext()) {
                        headPos = iterator.position();
                        Record next = iterator.next();
                        if (next.sequence == lastSequence) {
                            break;
                        }
                    }
                }
            }
            logger.info("Created iterator for node {}, from sequence {}, position {}", nodeId, lastSequence, headPos);
            tracker.put(nodeId, new LastEntry(lastSequence, headPos));
            commitTable.update(nodeId, lastSequence);
        }

        @Override
        public List<Record> fetch(String nodeId) {

            LastEntry entry = tracker.get(nodeId);
            commitTable.update(nodeId, entry.sequence);

            synchronized (this) {
                boolean completed;
                do {
                    completed = false;
                    ReplicationSignal signal = writeItems.peek();
                    if (signal != null) {
                        long replicated = commitTable.highWaterMark();
                        completed = signal.tryComplete(replicated);
                    }
                    if (completed) {
                        writeItems.poll();
                    }

                } while (completed);
            }


            try (LogIterator<Record> iterator = log.iterator(Direction.FORWARD, entry.headPos)) {
                List<Record> entries = new ArrayList<>(BATCH_SIZE);
                while (entries.size() < BATCH_SIZE && iterator.hasNext()) {
                    Record next = iterator.next();
                    if (next.sequence < entry.sequence) {
                        throw new RuntimeException("Non sequential replication entry");
                    }
                    entry.sequence = next.sequence;
                    entry.headPos = iterator.position();
                    entries.add(next);
                }
                return entries;
            }
        }
    }

    private static class ReplicationSignal {
        private final WorkItem<Long> workItem;
        private final ReplicationLevel writeLevel;
        private final long sequence;
        private final int clusterSize;
        private int hits;

        private ReplicationSignal(WorkItem<Long> workItem, ReplicationLevel writeLevel, long sequence, int clusterSize) {
            this.workItem = workItem;
            this.writeLevel = writeLevel;
            this.sequence = sequence;
            this.clusterSize = clusterSize;
        }

        private boolean tryComplete(long commitIndex) {
            if (commitIndex >= sequence) {
                hits++;
            }
            if (ReplicationLevel.ONE.equals(writeLevel)) {
                if (hits >= 1) {
                    complete();
                    return true;
                }
                return false;
            }
            if (ReplicationLevel.QUORUM.equals(writeLevel)) {
                if (hits >= (clusterSize / 2)) { //half cluster (plus the master) => n/2+1
                    complete();
                    return true;
                }
                return false;
            }
            if (ReplicationLevel.ALL.equals(writeLevel)) { //master
                if (hits == clusterSize - 1) {
                    complete();
                    return true;
                }
                return false;
            }
            return false;
        }

        private void complete() {
            workItem.complete(sequence);//returning sequence instead position
        }
    }

    private static class LastEntry {
        private long sequence;
        private long headPos;

        public LastEntry(long sequence, long headPos) {
            this.sequence = sequence;
            this.headPos = headPos;
        }
    }

}
