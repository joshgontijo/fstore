package io.joshworks.fstore.server.cluster.nodelog;

import io.joshworks.fstore.es.shared.EventRecord;
import io.joshworks.fstore.log.EventSerializer;
import io.joshworks.fstore.core.io.IOUtils;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.appender.FlushMode;
import io.joshworks.fstore.log.appender.LogAppender;

import java.io.Closeable;
import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

public class NodeLog implements Iterable<EventRecord>, Closeable {

    static final String NODES_STREAM = "NODES";
    private static final String NAME = "node-log";

    private final LogAppender<EventRecord> log;
    private final AtomicInteger version = new AtomicInteger();

    public NodeLog(File root) {
        this.log = LogAppender.builder(new File(root, NAME), new EventSerializer()).flushMode(FlushMode.ALWAYS)
                .segmentSize(Size.MB.of(20))
                .storageMode(StorageMode.RAF)
                .name(NAME)
                .open();

        this.version.set((int) log.entries());

    }

    public void append(NodeEvent event) {
        EventRecord record = event.toEvent();
        EventRecord withTimestamp = new EventRecord(record.stream, record.type, version.getAndIncrement(), System.currentTimeMillis(), record.data, record.metadata);
        log.append(withTimestamp);
    }

    @Override
    public LogIterator<EventRecord> iterator() {
        return log.iterator(Direction.FORWARD);
    }

    @Override
    public void close() {
        IOUtils.closeQuietly(log);
    }
}
