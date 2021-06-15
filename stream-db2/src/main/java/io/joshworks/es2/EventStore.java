package io.joshworks.es2;

import io.joshworks.es2.directory.CompactionResult;
import io.joshworks.es2.log.TLog;
import io.joshworks.es2.sink.Sink;
import io.joshworks.es2.sstable.SSTableConfig;
import io.joshworks.es2.sstable.SSTables;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.core.util.Threads;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class EventStore implements Closeable {

    private final MemTable memTable;
    private final SSTables sstables;
    private final TLog tlog;
    private final ExecutorService worker;
    private final DirLock dirLock;

    public EventStore(Path root, ExecutorService worker) {
        this.dirLock = new DirLock(root.toFile());
        this.worker = worker;
        this.sstables = new SSTables(root, new SSTableConfig(), worker);
        this.tlog = new TLog(root, worker);
        this.memTable = new MemTable(Size.MB.ofInt(10), true);
        this.loadMemTable();
    }

    private void loadMemTable() {

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

    public void append(ByteBuffer event) {
        int eventVersion = Event.version(event);
        long stream = Event.stream(event);

        int currVersion = version(stream);
        int nextVersion = currVersion + 1;
        if (eventVersion != -1 && eventVersion != nextVersion) {
            throw new RuntimeException("Version mismatch");
        }

        Event.writeVersion(event, nextVersion);

        tlog.append(event);
        event.flip();
        if (!memTable.add(event)) {
            memTable.flush(sstables);
            tlog.roll();

            memTable.add(event);
        }
    }

    @Override
    public void close() {
        try {
            Threads.awaitTermination(worker, Long.MAX_VALUE, TimeUnit.MILLISECONDS, () -> System.out.println("Awaiting termination..."));
        } finally {
            dirLock.close();
        }
    }

    public CompletableFuture<CompactionResult> compact() {
        return sstables.compact();
    }
}
