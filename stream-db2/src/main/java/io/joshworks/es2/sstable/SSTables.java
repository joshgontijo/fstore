package io.joshworks.es2.sstable;

import io.joshworks.es2.directory.SegmentDirectory;
import io.joshworks.es2.sink.Sink;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Iterator;

import static io.joshworks.es2.Event.NO_VERSION;

public class SSTables {

    private static final String DATA_EXT = "sst";
    private final SegmentDirectory<SSTable> sstables;

    public SSTables(Path folder) {
        sstables = new SegmentDirectory<>(folder.toFile(), DATA_EXT);
        sstables.loadSegments(SSTable::open);
    }

    public int get(long stream, int fromVersionInclusive, Sink sink) {
        try (SegmentDirectory<SSTable>.SegmentIterator it = sstables.iterator()) {
            while (it.hasNext()) {
                var res = it.next().get(stream, fromVersionInclusive, sink);
                if (res >= 0) {
                    return res;
                }
            }
        }
        return SSTable.NO_DATA;
    }

    public int version(long stream) {
        try (SegmentDirectory<SSTable>.SegmentIterator it = sstables.iterator()) {
            while (it.hasNext()) {
                int version = it.next().version(stream);
                if (version > NO_VERSION) {
                    return version;
                }
            }
            return NO_VERSION;
        }
    }

    public void flush(Iterator<ByteBuffer> iterator) {
        File headFile = sstables.newHead();
        SSTable sstable = SSTable.create(headFile, iterator);
        sstables.append(sstable);
    }

    public void delete() {
        sstables.delete();
    }

}
