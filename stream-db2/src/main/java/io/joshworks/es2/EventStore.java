package io.joshworks.es2;

import io.joshworks.es2.directory.CompactionResult;
import io.joshworks.es2.log.TLog;
import io.joshworks.es2.sink.Sink;
import io.joshworks.es2.sstable.SSTables;
import io.joshworks.fstore.core.seda.TimeWatch;
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

    EventStore(Path root, Builder builder) {
        System.out.println("Opening " + root);
        this.dirLock = new DirLock(root.toFile());
        this.worker = Executors.newFixedThreadPool(builder.compaction.threads());
        this.sstables = new SSTables(root, builder.compaction, worker);
        this.memTable = new MemTable(builder.memTable.size, builder.memTable.direct);
        this.tlog = TLog.open(root, builder.log.flushMode, builder.log.size, worker, memTable::add);
        this.writer = new BatchWriter(this, builder.writer.poolTimeMs, builder.writer.waitTimeout, builder.writer.maxEntries);

        System.out.println("Restored " + memTable.entries() + " entries");
    }

    public static boolean isFlushEvent(ByteBuffer record) {
        return Streams.STREAM_INTERNAL == Event.stream(record) && FLUSH_EVENT_TYPE.equals(Event.eventType(record));
    }

    public static Builder builder() {
        return new Builder();
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

    // must be called only from the batch-writer thread
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

        this.memTable.clear();

        System.out.println("Flushed " + entries + " entries (" + memTableSize + " bytes) in " + watch.elapsed() + "ms");
    }

    @Override
    public void close() {
        try {
            writer.close();
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


}
