package io.joshworks;

import io.joshworks.fstore.log.appender.LogAppender;
import io.joshworks.fstore.tcp.TcpClientConnection;
import io.joshworks.fstore.tcp.client.TcpEventClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static io.joshworks.ReplicatedLog.NO_SEQUENCE;

public class ReplicationClient implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(ReplicationClient.class);

    private final Thread thread;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final TcpClientConnection connection;
    private final int replicationTimeoutThreshold;
    private final LogAppender<Record> log;
    private final ReplicationRpc proxy;
    private final int fetchInterval; //used for when no data is found

    private final AtomicLong lastSequence = new AtomicLong(NO_SEQUENCE);
    private final String thisNode;
    private final CommitTable commitTable;

    public ReplicationClient(
            String thisNode,
            InetSocketAddress address,
            CommitTable commitTable,
            int fetchInterval,
            int replicationTimeoutThreshold,
            LogAppender<Record> log,
            long lastSequence) {

        this.thisNode = thisNode;
        this.commitTable = commitTable;
        this.lastSequence.set(lastSequence);
        this.fetchInterval = fetchInterval;
        this.replicationTimeoutThreshold = replicationTimeoutThreshold;
        this.log = log;
        this.connection = TcpEventClient.create().connect(address, 5, TimeUnit.SECONDS);
        this.proxy = connection.createRpcProxy(ReplicationRpc.class, 10_000, false);
        this.thread = new Thread(new Worker(), "replication-worker");
        this.thread.start();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        try {
            thread.join();
            connection.close();
        } catch (InterruptedException e) {
            logger.error("Failed to wait for replication thread to stop");
            Thread.currentThread().interrupt();
        }
    }

    private class Worker implements Runnable {

        @Override
        public void run() {

            proxy.createIterator(thisNode, lastSequence.get());

            while (!closed.get()) {
                //TODO logstore should support bulk insert
                long start = System.currentTimeMillis();
                List<Record> records = proxy.fetch(thisNode);
                if (System.currentTimeMillis() - start > replicationTimeoutThreshold) {
                    logger.warn("Fetch request took more than " + replicationTimeoutThreshold);
                }
//                System.out.println(nodeId + "-> fetched " + records.size());
                for (Record record : records) {
                    log.append(record);
                    long expectedSeq = record.sequence - 1;
                    if (!lastSequence.compareAndSet(expectedSeq, record.sequence)) {
                        throw new RuntimeException("Expected current sequence: " + expectedSeq + " actual " + lastSequence.get());
                    }
                    commitTable.update(thisNode, record.sequence);
                }
//                if (records.isEmpty()) {
//                    Threads.sleep(fetchInterval);
//                }
            }
        }
    }

}
