package io.joshworks.es2.sstable;

import io.joshworks.es2.directory.SegmentDirectory;
import io.joshworks.es2.sink.Sink;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

import static io.joshworks.es2.Event.NO_VERSION;
import static io.joshworks.es2.Event.VERSION_TOO_HIGH;

public class SSTables {

    private static final String DATA_EXT = "sst";
    private final SegmentDirectory<SSTable> sstables;

    public SSTables(Path folder, ExecutorService executor) {
        sstables = new SegmentDirectory<>(folder.toFile(), SSTable::open, DATA_EXT, executor, new SSTableCompaction());
        sstables.loadSegments();
    }

    public int get(long stream, int fromVersionInclusive, Sink sink) {
        try (var view = sstables.view()) {
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
        try (var view = sstables.view()) {
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

    public void flush(Iterator<ByteBuffer> iterator) {
        var headFile = sstables.newHead();
        var sstable = SSTable.create(headFile, iterator);
        sstables.append(sstable);
    }

    public void delete() {
        sstables.delete();
    }

    public CompletableFuture<Void> compact() {
        return sstables.compact(2, 2);
    }

}
