package io.joshworks.es2;

import io.joshworks.es2.directory.CompactionResult;
import io.joshworks.es2.log.TLog;
import io.joshworks.es2.sink.Sink;
import io.joshworks.es2.sstable.SSTableConfig;
import io.joshworks.es2.sstable.SSTables;
import io.joshworks.fstore.core.seda.TimeWatch;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.core.util.Threads;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class EventStore implements Closeable {

    //flush
    public static final String FLUSH_EVENT_TYPE = "FLUSH";
    final TLog tlog;
    final SSTables sstables;
    private final ExecutorService worker;
    private final DirLock dirLock;
    private final BatchWriter writer;
    MemTable memTable;

    private EventStore(Builder builder) {
        System.out.println("Opening " + builder.root);
        this.dirLock = new DirLock(builder.root.toFile());
        this.worker = Executors.newFixedThreadPool(builder.compactionThreads);
        this.sstables = new SSTables(builder.root, new SSTableConfig(), worker);
        this.memTable = new MemTable(builder.memtableSize, builder.memtableDirectBuffer);
        this.tlog = TLog.open(builder.root, builder.transactionLogSize, worker, memTable::add);
        this.writer = new BatchWriter(this, builder.batchWriterPoolTimeMs, builder.batchWriterTimeout, builder.batchMaxEntries);

        System.out.println("Restored " + memTable.entries() + " entries");
    }

    public static boolean isFlushEvent(ByteBuffer record) {
        return Streams.STREAM_INTERNAL == Event.stream(record) && FLUSH_EVENT_TYPE.equals(Event.eventType(record));
    }

    public static Builder open(Path path) {
        return new Builder(path);
    }

    public int version(long stream) {
        int currVersion = memTable.version(stream);
        if (currVersion == Event.NO_VERSION) {
            return sstables.version(stream);
        }
        return currVersion;
    }

    public int read(long stream, int startVersion, Sink sink) {
        int read = memTable.get(stream, startVersion, sink);
        if (read > 0 || read == Event.VERSION_TOO_HIGH) {
            return read;
        }
        return sstables.get(stream, startVersion, sink);
    }

    public CompletableFuture<Integer> append(ByteBuffer event) {
        if (!Event.isValid(event)) {
            return CompletableFuture.failedFuture(new RuntimeException("Invalid event"));
        }
        return writer.write(event);
    }

    void roll() {
        var watch = TimeWatch.start();
        int entries = memTable.entries();
        int memTableSize = memTable.size();
        if (entries == 0) {
            return;
        }
        var newSStable = sstables.flush(memTable.flushIterator(), entries, memTableSize);


        //WORST CASE SCENARIO HERE IS: The sstable is created and the log failed to append a flush event
        //On restart the memtable will reload with already flushed events causing it to flush it again
        //result would duplicate entries in another sstable, which is not a problem as these entries would be purged on compaction
        //TODO - check if compaction does in fact ignore duplicated entries during merge
        sstables.completeFlush(newSStable);

        this.memTable = new MemTable(Size.MB.ofInt(10), true);

        System.out.println("Flushed " + entries + " entries (" + memTableSize + " bytes) in " + watch.elapsed() + "ms");
    }

    @Override
    public void close() {
        try {
            Threads.awaitTermination(worker, Long.MAX_VALUE, TimeUnit.MILLISECONDS, () -> System.out.println("Awaiting termination..."));
        } finally {
            dirLock.close();
            sstables.close();
            tlog.close();
        }
    }

    public CompletableFuture<CompactionResult> compact() {
        return sstables.compact();
    }


    public static class Builder {
        private final Path root;
        private long transactionLogSize = Size.MB.of(100);
        private int memtableSize = Size.MB.ofInt(50);
        private boolean memtableDirectBuffer = false;
        private int compactionThreads = 3;

        //writer
        private long batchWriterPoolTimeMs = 2;
        private long batchWriterTimeout = 50;
        private int batchMaxEntries = 1000;

        public Builder(Path root) {
            this.root = root;
        }

        public Builder batchWriterPoolTimeMs(long batchWriterPoolTimeMs) {
            this.batchWriterPoolTimeMs = batchWriterPoolTimeMs;
            return this;
        }

        public Builder batchWriterTimeout(long batchWriterTimeout) {
            this.batchWriterTimeout = batchWriterTimeout;
            return this;
        }

        public Builder batchMaxEntries(int batchMaxEntries) {
            this.batchMaxEntries = batchMaxEntries;
            return this;
        }

        public Builder compactionThreads(int compactionThreads) {
            this.compactionThreads = compactionThreads;
            return this;
        }

        public Builder memtableSize(int memtableSize) {
            this.memtableSize = memtableSize;
            return this;
        }

        public Builder memtableDirectBuffer(boolean memtableDirectBuffer) {
            this.memtableDirectBuffer = memtableDirectBuffer;
            return this;
        }

        public Builder transactionLogSize(long transactionLogSize) {
            this.transactionLogSize = transactionLogSize;
            return this;
        }

        public EventStore build() {
            return new EventStore(this);
        }

    }

}
