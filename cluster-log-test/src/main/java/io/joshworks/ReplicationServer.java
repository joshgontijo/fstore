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

public class ReplicationServer implements Closeable {

    private static final int BATCH_SIZE = 10;

    private static final Logger logger = LoggerFactory.getLogger(ReplicationServer.class);
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Map<String, LogIterator<Record>> iterators = new ConcurrentHashMap<>();
    private final TcpMessageServer server;
    private final LogAppender<Record> log;

    public ReplicationServer(InetSocketAddress address, LogAppender<Record> log) {
        this.log = log;
        this.server = TcpMessageServer.create()
                .onOpen(conn -> {
                    //TODO if this node is not a master, then throws an exception
                    logger.info("Started replication server");
                })
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

    private class ReplicationHandler implements ReplicationRpc {

        @Override
        public void createIterator(long position, String nodeId) {
            position = Math.max(position, Log.START);
            LogIterator<Record> iterator = log.iterator(Direction.FORWARD, position);
            iterators.put(nodeId, iterator);
        }

        @Override
        public List<Record> fetch(String nodeId) {
            LogIterator<Record> iterator = iteratorFor(nodeId);

            List<Record> entries = new ArrayList<>(BATCH_SIZE);
            while (entries.size() < BATCH_SIZE && iterator.hasNext()) {
                entries.add(iterator.next());
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
