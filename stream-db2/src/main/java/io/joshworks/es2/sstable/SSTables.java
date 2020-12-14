package io.joshworks.es2.sstable;

import io.joshworks.es2.directory.SegmentDirectory;
import io.joshworks.es2.index.IndexEntry;

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

    public IndexEntry get(long stream, int fromVersionInclusive) {
        for (SSTable sstable : sstables) {
            var ie = sstable.get(stream, fromVersionInclusive);
            if (ie != null) {
                return ie;
            }
        }
        return null;
    }

    public int version(long stream) {
        for (SSTable sstable : sstables) {
            int version = sstable.version(stream);
            if (version > NO_VERSION) {
                return version;
            }
        }
        return NO_VERSION;
    }


    public void flush(Iterator<ByteBuffer> iterator) {
        File headFile = sstables.newHead();
        SSTable sstable = SSTable.create(headFile, iterator);
        sstables.append(sstable);
    }


}
