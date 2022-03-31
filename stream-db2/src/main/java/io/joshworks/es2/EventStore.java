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
import java.util.concurrent.TimeUnit;

public class EventStore implements Closeable {

    MemTable memTable;
    final TLog tlog;
    final SSTables sstables;
    private final ExecutorService worker;
    private final DirLock dirLock;
    private static final long TLOG_SIZE = Size.MB.of(100); // TODO parameter
    private static final long BATCH_POOL_TIME_MS = 1; // TODO parameter
    private static final long BATCH_TIMEOUT = 50; // TODO parameter
    private static final int BATCH_MAX_ENTRIES = 1000; // TODO parameter

    private final BatchWriter writer;

    //flush
    public static final String FLUSH_EVENT_TYPE = "FLUSH";

    public static boolean isFlushEvent(ByteBuffer record) {
        return Streams.STREAM_INTERNAL == Event.stream(record) && FLUSH_EVENT_TYPE.equals(Event.eventType(record));
    }

    public EventStore(Path root, ExecutorService worker) {
        this.dirLock = new DirLock(root.toFile());
        this.worker = worker;
        this.sstables = new SSTables(root, new SSTableConfig(), worker);
        this.memTable = new MemTable(Size.MB.ofInt(10), true);
        this.tlog = TLog.open(root, TLOG_SIZE, worker, memTable::add);
        this.writer = new BatchWriter(this, BATCH_POOL_TIME_MS, BATCH_TIMEOUT, BATCH_MAX_ENTRIES);

        System.out.println("Restored " + memTable.entries() + " entries");
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

    synchronized void roll() {
        var watch = TimeWatch.start();
        int entries = memTable.entries();
        int memTableSize = memTable.size();

        var newSStable = sstables.flush(memTable.flushIterator(), entries, memTableSize);


        //WORST CASE SCENARIO HERE IS: The sstable is created and the log failed to append a flush event
        //On restart the memtable will reload with already flushed events causing it to flush it again
        //result would duplicate entries in another sstable, which is not a problem as these entries would be purged on compaction
        //TODO check if compaction does in fact ignore duplicated entries during merge
        sstables.completeFlush(newSStable);

        var oldMemTable = this.memTable;
        this.memTable = new MemTable(Size.MB.ofInt(10), true);
        oldMemTable.clear();

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

}
