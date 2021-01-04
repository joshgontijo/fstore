package io.joshworks.es2.log;

import io.joshworks.es2.Event;
import io.joshworks.es2.SegmentChannel;
import io.joshworks.es2.directory.SegmentDirectory;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;

public class TLog {

    private static final String EXT = "log";
    private final SegmentDirectory<SegmentChannel> logs;
    private final AtomicLong sequence = new AtomicLong(-1);
    private SegmentChannel head;

    public TLog(Path folder) {
        this.logs = new SegmentDirectory<>(folder.toFile(), EXT);
        this.head = SegmentChannel.create(logs.newHead());
    }

    public void append(ByteBuffer data) {
        long sequence = this.sequence.incrementAndGet();
        long timestamp = System.currentTimeMillis();

        Event.writeTimestamp(data, timestamp);
        Event.writeSequence(data, sequence);

        head.append(data);
    }

    public void roll() {
        head.truncate();
        logs.append(head);
        head = SegmentChannel.create(logs.newHead());
    }
}
