package io.joshworks.es2.log;

import io.joshworks.es2.SegmentChannel;
import io.joshworks.es2.directory.Compaction;
import io.joshworks.es2.directory.CompactionItem;
import io.joshworks.es2.directory.SegmentDirectory;
import io.joshworks.es2.directory.View;
import io.joshworks.fstore.core.io.buffers.Buffers;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * REC_SIZE (4 bytes)
 * CRC_CHECKSUM (4 bytes)
 * SEQUENCE (8 bytes)
 * TYPE (1 byte)
 */
public class TLog implements Closeable {

    private static final String EXT = "log";
    private static final long START_SEQUENCE = -1;
    public static final int ENTRY_PART_COUNT = 3; // header + data + footer
    private final SegmentDirectory<SegmentChannel> logs;
    private final ByteBuffer sequenceBuf = Buffers.allocate(Long.BYTES, false);
    private final AtomicLong sequence = new AtomicLong(START_SEQUENCE);
    private final BatchWriter writer;
    private final long maxSize;
    private SegmentChannel head;

    static final int HEADER_SIZE = Integer.BYTES * 2 + Long.BYTES + Byte.BYTES; //rec_size + crc + sequence + type
    static final int TYPE_OFFSET = Integer.BYTES * 2 + Long.BYTES;
    static final int SEQUENCE_OFFSET = Integer.BYTES * 2;
    static final int FOOTER_SIZE = Integer.BYTES; //rec_size

    private TLog(Path folder, long maxSize, ExecutorService executor) {
        this.maxSize = maxSize;
        this.writer = new BatchWriter(this, 100, 200);
        this.logs = new SegmentDirectory<>(folder.toFile(), SegmentChannel::open, EXT, executor, new TLogCompaction());
    }

    public static TLog open(Path folder, long maxSize, ExecutorService executor, Consumer<ByteBuffer> fn) {
        var tlog = new TLog(folder, maxSize, executor);
        try (var view = tlog.logs.view()) {
            if (view.isEmpty()) {
                return tlog;
            }
            TLogRestorer.restore(view, fn);
            tlog.sequence.set(findLastSequence(view));
        }
        return tlog;
    }

    private static long findLastSequence(View<SegmentChannel> view) {
        var it = view.reverse();
        while (it.hasNext()) {
            long lastSequence = lastSequence(it.next());
            if (lastSequence > START_SEQUENCE) {
                return lastSequence;
            }
        }
        return START_SEQUENCE;
    }

    public synchronized CompletableFuture<Long> append(ByteBuffer[] entries) {
        if (entries.length == 0) {
            throw new IllegalArgumentException("Buffer entries must not be empty");
        }

        tryCreateNewLog();

        CompletableFuture<Long> future = CompletableFuture.completedFuture(Long.MAX_VALUE);
        for (ByteBuffer data : entries) {
            var task = writer.submit(data, sequence.incrementAndGet(), Type.DATA);
            future = future.thenCombine(task, Math::min);
        }
        return future;
    }

    private void tryCreateNewLog() {
        if (head == null) { //lazy initialization so we run restore logic
            this.head = SegmentChannel.create(logs.newHead());
            logs.append(head);
        }
        if (head.size() >= maxSize) {
            roll();
        }
    }

    public synchronized CompletableFuture<Long> append(ByteBuffer data) {
        tryCreateNewLog();
        return writer.submit(data, sequence.incrementAndGet(), Type.DATA);
    }


    synchronized void roll() {
        head.truncate();
        head.flush();
        head = SegmentChannel.create(logs.newHead());
        logs.append(head);
    }

    public synchronized CompletableFuture<Long> appendFlushEvent() {
        if (head.size() >= maxSize) {
            roll();
        }
        long seq = sequence.get();
        sequenceBuf.clear().putLong(seq);
        return writer.submit(sequenceBuf, sequence.incrementAndGet(), Type.DATA);
    }

    private static long firstSequence(SegmentChannel channel) {
        var seqBuf = Buffers.allocate(Long.BYTES, false);
        int read = channel.read(seqBuf, SEQUENCE_OFFSET);
        if (read != Long.BYTES) {
            return START_SEQUENCE;
        }
        var sequence = seqBuf.flip().getLong();
        assert sequence >= 0;
        return sequence;
    }

    private static long lastSequence(SegmentChannel channel) {
        var sizeBuff = Buffers.allocate(Integer.BYTES, false);
        long position = channel.position();
        if (position < Integer.BYTES) {
            return START_SEQUENCE;
        }
        int read = channel.read(sizeBuff, position - Integer.BYTES);
        if (read != Integer.BYTES) {
            return START_SEQUENCE;
        }
        var entrySize = sizeBuff.flip().getInt();
        var seqBuf = Buffers.allocate(Long.BYTES, false);
        channel.read(seqBuf, position - entrySize + SEQUENCE_OFFSET);
        var sequence = seqBuf.flip().getLong();
        assert sequence >= 0;
        return sequence;
    }

    public long sequence() {
        return sequence.get();
    }

    @Override
    public synchronized void close() {
        logs.close();
    }

    private static class TLogCompaction implements Compaction<SegmentChannel> {

        @Override
        public void compact(CompactionItem<SegmentChannel> handle) {

        }
    }

}
