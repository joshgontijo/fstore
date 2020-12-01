package io.joshworks.es2.sstable;

import io.joshworks.es2.directory.SegmentDirectory;
import io.joshworks.es2.directory.View;
import io.joshworks.es2.index.IndexEntry;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Iterator;

import static io.joshworks.es2.Event.NO_VERSION;

public class SSTables {

    private static final String DATA_EXT = "sst";
    private final Path folder;
    private final SegmentDirectory<SSTable> sstables;

    public SSTables(Path folder) {
        this.folder = folder;
        sstables = new SegmentDirectory<>(folder.toFile(), DATA_EXT);
        sstables.loadSegments(SSTable::open);
    }

    private void openFiles() {
//        for (File file : requireNonNull(folder.toFile().listFiles())) {
//            if (!file.getName().endsWith(DATA_EXT)) {
//                continue;
//            }
//
//
//            SSTable.open(file);
//        }
    }

    public IndexEntry get(long stream, int fromVersionInclusive) {
        try (View<SSTable> view = sstables.view()) {
            for (SSTable sstable : view) {
                var ie = sstable.get(stream, fromVersionInclusive);
                if (ie != null) {
                    return ie;
                }
            }
            return null;
        }
    }

    public int version(long stream) {
        try (View<SSTable> view = sstables.view()) {
            for (SSTable sstable : view) {
                int version = sstable.version(stream);
                if (version > NO_VERSION) {
                    return version;
                }
            }
            return NO_VERSION;
        }
    }


    public void flush(Iterator<ByteBuffer> iterator) {


    }


}
