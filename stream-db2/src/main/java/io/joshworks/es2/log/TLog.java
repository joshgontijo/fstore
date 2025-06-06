package io.joshworks.es2.log;

import io.joshworks.es2.Builder;
import io.joshworks.es2.SegmentChannel;
import io.joshworks.es2.directory.Compaction;
import io.joshworks.es2.directory.CompactionItem;
import io.joshworks.es2.directory.SegmentDirectory;
import io.joshworks.fstore.core.iterators.CloseableIterator;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class TLog implements Closeable, Iterable<ByteBuffer> {

    static final long NO_SEQUENCE = -1;
    private static final String EXT = "log";
    private final SegmentDirectory<SegmentChannel> logs;
    private final AtomicLong sequence = new AtomicLong(NO_SEQUENCE);
    private final Builder.FlushMode flushMode;
    private final long maxSize;
    private SegmentChannel head;


    private TLog(Path folder, Builder.FlushMode flushMode, long maxSize, ExecutorService executor) {
        this.flushMode = flushMode;
        this.maxSize = maxSize;
        this.logs = new SegmentDirectory<>(folder.toFile(), SegmentChannel::open, EXT, executor, new TLogCompaction());
    }

    public static TLog open(Path folder, Builder.FlushMode flushMode, long maxSize, ExecutorService executor, Consumer<ByteBuffer> fn) {
        var tlog = new TLog(folder, flushMode, maxSize, executor);
        try (var view = tlog.logs.view()) {
            if (view.isEmpty()) {
                return tlog;
            }
            long lastSequence = TLogRestorer.restore(view, fn);
            tlog.sequence.set(lastSequence);
        }
        return tlog;
    }

    public synchronized void append(ByteBuffer[] entries, int count) {
        tryCreateNewHead();
        head.append(entries, count);
        sequence.addAndGet(entries.length);
    }

    public synchronized void append(ByteBuffer data) {
        tryCreateNewHead();
        head.append(data);
        if (Builder.FlushMode.ON_WRITE.equals(flushMode)) {
            head.flush();
        }
        sequence.incrementAndGet();
    }

    private void tryCreateNewHead() {
        if (head == null) { //lazy initialization so we run restore logic
            this.head = SegmentChannel.create(logs.newHead());
            logs.append(head);
        }
        if (head.size() >= maxSize) {
            roll();
        }
    }

    synchronized void roll() {
        head.flush();
        head.truncate();
        head = SegmentChannel.create(logs.newHead());
        logs.append(head);
    }

    public long sequence() {
        return sequence.get();
    }

    @Override
    public synchronized void close() {
        logs.close();
    }

    @Override
    public CloseableIterator<ByteBuffer> iterator() {
        return new TLogIterator(logs);
    }

    private static class TLogCompaction implements Compaction<SegmentChannel> {

        @Override
        public void compact(CompactionItem<SegmentChannel> handle) {

        }
    }


}
