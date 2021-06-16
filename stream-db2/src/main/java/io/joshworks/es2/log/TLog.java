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

    public CompletableFuture<Void> append(ByteBuffer data) {
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
        private final CompletableFuture<Void> future;
        private final ByteBuffer data;
        private boolean poisonPill;

        private WriteItem(CompletableFuture<Void> future, ByteBuffer data) {
            this.future = future;
            this.data = data;
        }

        private void complete() {
            future.complete(null);
        }
    }

    private class Writer implements Runnable {

        private final ByteBuffer buffer;
        private final long timeThreshold;
        private final BlockingQueue<WriteItem> items = new ArrayBlockingQueue<>(1000);
        private final List<WriteItem> bufferedItems = new ArrayList<>();

        Writer(int capacity, long timeThreshold) {
            this.buffer = Buffers.allocate(capacity, false);
            this.timeThreshold = timeThreshold;
        }

        public CompletableFuture<Void> enqueue(ByteBuffer data) {
            try {
                var future = new CompletableFuture<Void>();
                items.put(new WriteItem(future, data));
                return future;

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Cannot add to write queue", e);
            }
        }

        public CompletableFuture<Void> stop() {
            try {
                var future = new CompletableFuture<Void>();
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
                    while (item != null) {
                        if (item.poisonPill) {
                            tryFlushBuffered();
                            item.complete();
                            return;
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
                        item = items.poll();
                    }
                    tryFlushBuffered();

                } catch (Exception e) {
                    throw new RuntimeException("Failed to write batch", e);
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

            buffer.put(item.data);
            bufferedItems.add(item);
            return true;
        }

        private void flush(ByteBuffer buf) {
            var head = TLog.this.head;
            int bufSize = buf.remaining();
            if (bufSize > 0) {
                long pos = head.append(buf);
                for (WriteItem item : bufferedItems) {
                    item.complete();
                }
                bufferedItems.clear();
            }
            if (head.position() >= segmentSize) {
                TLog.this.roll();
            }
        }
    }

}
