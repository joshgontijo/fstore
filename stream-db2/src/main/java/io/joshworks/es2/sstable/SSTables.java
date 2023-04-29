package io.joshworks.es2.sstable;

import io.joshworks.es2.directory.CompactionResult;
import io.joshworks.es2.directory.SegmentDirectory;
import io.joshworks.es2.sink.Sink;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static io.joshworks.es2.Event.NO_VERSION;
import static io.joshworks.es2.Event.VERSION_TOO_HIGH;

public class SSTables implements Closeable {

    private static final String DATA_EXT = "sst";
    private final SegmentDirectory<SSTable> items;

    private final CompactionConfig config;

    public SSTables(Path folder, CompactionConfig config, ExecutorService executor) {
        this.config = config.copy();
        items = new SegmentDirectory<>(folder.toFile(), SSTable::open, DATA_EXT, executor, new SSTableCompaction(config.copy()));
    }

    public int get(long stream, int fromVersionInclusive, Sink sink) {
        try (var view = items.view()) {
            for (int i = 0; i < view.size(); i++) {
                var sstable = view.get(i);
                var res = sstable.get(stream, fromVersionInclusive, sink);
                if (res >= 0 || res == VERSION_TOO_HIGH) {
                    return res;
                }
            }
        }
        return SSTable.NO_DATA;
    }

    public int version(long stream) {
        try (var view = items.view()) {
            for (int i = 0; i < view.size(); i++) {
                var sstable = view.get(i);
                int version = sstable.version(stream);
                if (version > NO_VERSION) {
                    return version;
                }
            }
        }
        return NO_VERSION;
    }

    public SSTable flush(Iterator<ByteBuffer> iterator, int entryCount, long size) {
        var headFile = items.newHead();
        return SSTable.create(headFile, iterator, entryCount, size, config.lowConfig);
    }

    public CompletableFuture<CompactionResult> completeFlush(SSTable ssTable) {
        items.append(ssTable);
        return compact();
    }

    public void delete() {
        items.delete();
    }

    public CompletableFuture<CompactionResult> compact() {
        return items.compact(config);
    }

    @Override
    public synchronized void close() {
        items.close();
    }
}
