package io.joshworks.es2.log;

import io.joshworks.es2.Event;
import io.joshworks.es2.SegmentChannel;
import io.joshworks.es2.directory.Compaction;
import io.joshworks.es2.directory.CompactionItem;
import io.joshworks.es2.directory.SegmentDirectory;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.core.util.Threads;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class TLog {

    private static final String EXT = "log";
    private final SegmentDirectory<SegmentChannel> logs;
    //TODO load on restore
    private final AtomicLong sequence = new AtomicLong(0);
    private SegmentChannel head;
    private final Writer writer = new Writer(Size.KB.ofInt(64), 1);
    private final long segmentSize;
    private final Thread writeWorker = Threads.named("log-writer", writer);

    public TLog(Path folder, ExecutorService executor, long segmentSize) {
        this.segmentSize = segmentSize;
        this.logs = new SegmentDirectory<>(folder.toFile(), SegmentChannel::open, EXT, executor, new TLogCompaction());
        this.writeWorker.start();
    }

    public void restore() {

    }

    public CompletableFuture<Long> append(ByteBuffer data) {
        if (head == null) { //lazy initialization so we run restore logic
            this.head = SegmentChannel.create(logs.newHead());
        }
        return writer.enqueue(data);
    }

    private void writeEntryHeader(ByteBuffer data) {
        long sequence = this.sequence.getAndIncrement();
        long timestamp = System.currentTimeMillis();
        Event.writeTimestamp(data, timestamp);
        Event.writeSequence(data, sequence);
    }

    private void roll() {
        head.truncate();
        head.flush();
        logs.append(head);
        head = SegmentChannel.create(logs.newHead());
    }

    private static class TLogCompaction implements Compaction<SegmentChannel> {

        @Override
        public void compact(CompactionItem<SegmentChannel> handle) {

        }
    }

    private static class WriteItem {
        private final CompletableFuture<Long> future;
        private final ByteBuffer data;
        private long position = -1;
        private boolean poisonPill;

        private WriteItem(CompletableFuture<Long> future, ByteBuffer data) {
            this.future = future;
            this.data = data;
        }

        private void position(long position) {
            this.position = position;
        }

        private void complete() {
            future.complete(position);
        }
    }

    private class Writer implements Runnable {

        private final ByteBuffer buffer;
        private final long timeThreshold;
        private final BlockingQueue<WriteItem> items = new ArrayBlockingQueue<>(1000);
        private final List<WriteItem> bufferedItems = new ArrayList<>();
        private long logPos;

        Writer(int capacity, long timeThreshold) {
            this.buffer = Buffers.allocate(capacity, false);
            this.timeThreshold = timeThreshold;
        }

        public CompletableFuture<Long> enqueue(ByteBuffer data) {
            try {
                var future = new CompletableFuture<Long>();
                items.put(new WriteItem(future, data));
                return future;

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Cannot add to write queue", e);
            }
        }

        public CompletableFuture<Long> stop() {
            try {
                var future = new CompletableFuture<Long>();
                var item = new WriteItem(future, null);
                item.poisonPill = true;
                items.put(item);
                return future;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Cannot add to write queue", e);
            }
        }

        @Override
        public void run() {
            while (true) {
                try {
                    var item = items.poll(timeThreshold, TimeUnit.MILLISECONDS);
                    if (item != null) {
                        if (item.poisonPill) {
                            tryFlushBuffered();
                            item.complete();
                            break;
                        }
                        if (buffer.capacity() < item.data.remaining()) {
                            tryFlushBuffered();
                            flush(item.data);
                            item.complete();
                        }
                        if (!writeEntry(item)) {
                            flush(buffer.flip());
                            writeEntry(item);
                        }
                    } else {
                        tryFlushBuffered();
                    }

                } catch (Exception e) {
                    throw new RuntimeException("Failed to write batch");
                }
            }
        }

        private void tryFlushBuffered() {
            if (!bufferedItems.isEmpty()) {
                flush(buffer.flip());
                buffer.clear();
            }
        }

        private boolean writeEntry(WriteItem item) {
            ByteBuffer data = item.data;
            if (buffer.remaining() < item.data.remaining()) {
                return false;
            }

            TLog.this.writeEntryHeader(data);

            item.position(logPos + buffer.position());
            buffer.put(item.data);
            bufferedItems.add(item);
            return true;
        }

        private void flush(ByteBuffer buf) {
            var head = TLog.this.head;
            if (logPos != head.position()) {
                throw new RuntimeException("Log position mismatch");
            }
            int bufSize = buf.remaining();
            if (bufSize > 0) {
                long pos = head.append(buf);
                assert pos == logPos;
                for (WriteItem item : bufferedItems) {
                    item.complete();
                }
                bufferedItems.clear();
            }
            logPos += bufSize;
            if (head.position() >= segmentSize) {
                TLog.this.roll();
            }
        }
    }

}
