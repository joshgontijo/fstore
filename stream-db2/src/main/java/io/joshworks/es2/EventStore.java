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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class EventStore implements Closeable {

    private final MemTable memTable;
    private final SSTables sstables;
    private final TLog tlog;
    private final ExecutorService worker;
    private final DirLock dirLock;
    private static final long TLOG_SIZE = Size.MB.of(100); // TODO parameter

    public EventStore(Path root, ExecutorService worker) {
        this.dirLock = new DirLock(root.toFile());
        this.worker = worker;
        this.sstables = new SSTables(root, new SSTableConfig(), worker);
        this.memTable = new MemTable(Size.MB.ofInt(10), true);

        var restored = new AtomicInteger();
        this.tlog = TLog.open(root, TLOG_SIZE, worker, data -> {
            restored.incrementAndGet();
            memTable.add(data);
        });
        System.out.println("Restored " + restored.get() + " entries");
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

    public synchronized void append(ByteBuffer event) {
        int eventVersion = Event.version(event);
        long stream = Event.stream(event);

        int currVersion = version(stream);
        int nextVersion = currVersion + 1;
        if (eventVersion != -1 && eventVersion != nextVersion) {
            throw new RuntimeException("Version mismatch");
        }

        Event.writeVersion(event, nextVersion);
        Event.writeTimestamp(event, System.currentTimeMillis());

        tlog.append(event);
        event.flip();
        if (!memTable.add(event)) {
            roll();
            memTable.add(event);
        }
    }

    private synchronized void roll() {
        var watch = TimeWatch.start();
        int entries = memTable.entries();
        int memTableSize = memTable.size();

        var newSStable = sstables.flush(memTable.flushIterator(), entries, memTableSize);
        tlog.appendFlushEvent();
        sstables.completeFlush(newSStable);//append only after writing to tlog
        memTable.clear();

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
