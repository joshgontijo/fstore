package io.joshworks;

import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.appender.LogAppender;
import io.joshworks.fstore.tcp.TcpConnection;
import io.joshworks.fstore.tcp.TcpMessageServer;
import io.joshworks.fstore.tcp.server.TypedEventHandler;
import io.joshworks.tcp.CreateIterator;
import io.joshworks.tcp.IteratorCreated;
import io.joshworks.tcp.IteratorNext;
import io.joshworks.tcp.MessageBatch;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class TcpReplication<T> implements Closeable {

    private static final int BATCH_SIZE = 10;

    private final AtomicBoolean closed = new AtomicBoolean();
    private final Map<String, LogIterator<Entry<T>>> iterators = new ConcurrentHashMap<>();
    private final TcpMessageServer server;
    private final LogAppender<Entry<T>> log;
    //TODO
    private final AtomicLong highWaterMark = new AtomicLong();


    public TcpReplication(InetSocketAddress address, LogAppender<Entry<T>> log) {
        this.log = log;
        this.server = TcpMessageServer.create()
                .onOpen(conn -> {
                    //TODO if this node is not a master, then throws an exception
                })
                .onEvent(new EventHandler())
                .start(address);

    }

    @Override
    public void close() {
        if(!closed.compareAndSet(false, true)) {
            return;
        }
        server.close();
    }


    private class EventHandler extends TypedEventHandler {
        public EventHandler() {
            register(CreateIterator.class, this::createIterator);
            register(IteratorNext.class, this::iteratorNext);
        }

        private MessageBatch iteratorNext(TcpConnection conn, IteratorNext event) {
            LogIterator<Entry<T>> iterator = iterators.get(event.nodeId);
            if (iterator == null) {
                throw new RuntimeException("Iterator not yet created for node " + event.nodeId);
            }

            List<Entry<?>> entries = new ArrayList<>(BATCH_SIZE);
            while (entries.size() < BATCH_SIZE && iterator.hasNext()) {
                entries.add(iterator.next());
            }
            return new MessageBatch(entries);
        }

        private IteratorCreated createIterator(TcpConnection conn, CreateIterator event) {
            LogIterator<Entry<T>> iterator = log.iterator(Direction.FORWARD, event.position);
            iterators.put(event.nodeId, iterator);
            return new IteratorCreated();
        }


    }

}
