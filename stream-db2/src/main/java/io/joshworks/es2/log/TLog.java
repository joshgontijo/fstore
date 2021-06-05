package io.joshworks.es2.log;

import io.joshworks.es2.Event;
import io.joshworks.es2.SegmentChannel;
import io.joshworks.es2.directory.Compaction;
import io.joshworks.es2.directory.MergeHandle;
import io.joshworks.es2.directory.SegmentDirectory;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

public class TLog {

    private static final String EXT = "log";
    private final SegmentDirectory<SegmentChannel> logs;
    private final AtomicLong sequence = new AtomicLong(0);
    private SegmentChannel head;

    public TLog(Path folder, ExecutorService executor) {
        this.logs = new SegmentDirectory<>(folder.toFile(), SegmentChannel::open, EXT, executor, new TLogCompaction());
        this.head = SegmentChannel.create(logs.newHead());
    }

    public void append(ByteBuffer data) {
        long sequence = this.sequence.getAndIncrement();
        long timestamp = System.currentTimeMillis();

        Event.writeTimestamp(data, timestamp);
        Event.writeSequence(data, sequence);

        head.append(data);
    }

    public void roll() {
        head.truncate();
        head.force(false);
        logs.append(head);
        head = SegmentChannel.create(logs.newHead());
    }

    private static class TLogCompaction implements Compaction<SegmentChannel> {

        @Override
        public void compact(MergeHandle<SegmentChannel> handle) {

        }
    }

}
