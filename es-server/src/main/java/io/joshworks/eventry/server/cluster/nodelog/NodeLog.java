package io.joshworks.eventry.server.cluster.nodelog;

import io.joshworks.eventry.log.EventRecord;
import io.joshworks.eventry.log.EventSerializer;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.Direction;
import io.joshworks.fstore.log.LogIterator;
import io.joshworks.fstore.log.appender.FlushMode;
import io.joshworks.fstore.log.appender.LogAppender;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

public class NodeLog implements Iterable<EventRecord> {

    static final String PARTITIONS_STREAM = "PARTITIONS";
    static final String NODES_STREAM = "NODES";
    private static final String NAME = "node-log";

    private final LogAppender<EventRecord> log;

    public NodeLog(File root) {
        this.log = LogAppender.builder(new File(root, NAME), new EventSerializer()).flushMode(FlushMode.ALWAYS)
                .disableCompaction()
                .segmentSize(Size.MB.of(20))
                .storageMode(StorageMode.RAF)
                .name(NAME)
                .open();
    }

    public void append(NodeEvent event) {
        EventRecord record = event.toEvent();
        EventRecord withTimestamp = new EventRecord(record.stream, record.type, -1, System.currentTimeMillis(), record.body, record.metadata);
        log.append(withTimestamp);
    }

    public Set<Integer> ownedPartitions() {
        LogIterator<EventRecord> it = iterator();

        Set<Integer> partitions = new HashSet<>();
        while (it.hasNext()) {
            EventRecord entry = it.next();
            switch (entry.type) {
                case PartitionCreatedEvent.TYPE:
                    partitions.add(PartitionCreatedEvent.from(entry).id);
                case PartitionMovedEvent.TYPE:
                    partitions.remove(PartitionMovedEvent.from(entry).id);
            }
        }
        return partitions;
    }

    @Override
    public LogIterator<EventRecord> iterator() {
        return log.iterator(Direction.FORWARD);
    }
}
