package io.joshworks.es2.log;

import io.joshworks.es2.SegmentChannel;
import io.joshworks.es2.directory.Compaction;
import io.joshworks.es2.directory.CompactionItem;
import io.joshworks.es2.directory.SegmentDirectory;
import io.joshworks.es2.directory.View;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.ByteBufferChecksum;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * REC_SIZE (4 bytes)
 * CRC_CHECKSUM (4 bytes)
 * SEQUENCE (8 bytes)
 * TYPE (1 byte)
 *
 * [DATA] (n bytes)
 *
 * REC_SIZE (4 bytes)
 */
public class TLog implements Closeable {

    private static final String EXT = "log";
    static final long START_SEQUENCE = 0;
    public static final int ENTRY_PART_SIZE = 3;
    private final SegmentDirectory<SegmentChannel> logs;
    private final ByteBuffer[] writeBuffers = createWriteBuffers();
    private final ByteBuffer sequenceBuf = Buffers.allocate(Long.BYTES, false);
    private final AtomicLong sequence = new AtomicLong(START_SEQUENCE);
    private final long maxSize;
    private SegmentChannel head;

    public static final int MAX_BATCH_ENTRIES = 100;
    static final int HEADER_SIZE = Integer.BYTES * 2 + Long.BYTES + Byte.BYTES; //rec_size + crc + sequence + type
    static final int TYPE_OFFSET = Integer.BYTES * 2 + Long.BYTES;
    static final int SEQUENCE_OFFSET = Integer.BYTES * 2;
    static final int FOOTER_SIZE = Integer.BYTES; //rec_size

    private TLog(Path folder, long maxSize, ExecutorService executor) {
        this.maxSize = maxSize;
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

    public synchronized void append(ByteBuffer[] entries, int offset, int count) {
        tryCreateNewHead();

        int batchItems = 0;
        for (int i = offset; i < count; i++) {
            long seq = sequence.get() + i;
            composeEntry(entries[i], Type.DATA, seq, batchItems * ENTRY_PART_SIZE);
            if (++batchItems >= MAX_BATCH_ENTRIES / ENTRY_PART_SIZE) {//buffer full
                head.append(writeBuffers, 0, batchItems * ENTRY_PART_SIZE);
                sequence.addAndGet(batchItems);
                batchItems = 0;
            }
        }

        if (batchItems > 0) {
            head.append(writeBuffers, 0, batchItems * ENTRY_PART_SIZE);
        }
        sequence.addAndGet(count);
    }

    public synchronized void append(ByteBuffer data) {
        tryCreateNewHead();

        long seq = sequence.get() + 1;
        composeEntry(data, Type.DATA, seq, 0);
        head.append(writeBuffers);
        sequence.incrementAndGet();
        writeBuffers[1] = null;
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

    private void composeEntry(ByteBuffer data, Type type, long sequence, int buffOffset) {
        if (data.remaining() > maxSize) {
            throw new RuntimeException("Data too big");
        }
        if (buffOffset % ENTRY_PART_SIZE != 0) {
            throw new IllegalArgumentException("Invalid buffer offset");
        }

        int eventSize = data.remaining();
        int recSize = eventSize + HEADER_SIZE + FOOTER_SIZE;
        writeBuffers[buffOffset]
                .clear()
                .putInt(recSize)
                .putInt(ByteBufferChecksum.crc32(data, data.position(), eventSize))
                .putLong(sequence)
                .put(type.i)
                .flip();

        writeBuffers[buffOffset + 1] = data;

        writeBuffers[buffOffset + 2]
                .clear()
                .putInt(recSize)
                .flip();
    }

    synchronized void roll() {
        head.truncate();
        head.flush();
        head = SegmentChannel.create(logs.newHead());
        logs.append(head);
    }

    public synchronized void appendFlushEvent() {
        if (head.size() >= maxSize) {
            roll();
        }
        long seq = sequence.get();
        sequenceBuf.clear().putLong(seq);
        composeEntry(sequenceBuf, Type.FLUSH, seq + 1, 0);
        head.append(writeBuffers, 0, 1);
    }

    private static ByteBuffer[] createWriteBuffers() {
        var items = new ByteBuffer[MAX_BATCH_ENTRIES * ENTRY_PART_SIZE];
        for (var i = 0; i < items.length; i += ENTRY_PART_SIZE) {
            items[i] = Buffers.allocate(HEADER_SIZE, false); //header
            items[i + 1] = null; //data
            items[i + 2] = Buffers.allocate(FOOTER_SIZE, false); // footer
        }
        return items;
    }

    private static long firstSequence(SegmentChannel channel) {
        var seqBuf = Buffers.allocate(Long.BYTES, false);
        int read = channel.read(seqBuf, SEQUENCE_OFFSET);
        if (read != Long.BYTES) {
            return START_SEQUENCE;
        }
        long sequence = seqBuf.flip().getLong();
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
