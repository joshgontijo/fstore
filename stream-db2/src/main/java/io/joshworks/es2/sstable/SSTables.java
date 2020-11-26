package io.joshworks.es2.sstable;

import io.joshworks.es2.Event;
import io.joshworks.es2.SegmentChannel;
import io.joshworks.es2.index.IndexEntry;
import io.joshworks.es2.index.IndexWriter;
import io.joshworks.fstore.codec.snappy.SnappyCodec;
import io.joshworks.fstore.core.codec.Codec;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.Memory;
import io.joshworks.fstore.core.util.TestUtils;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.concurrent.CopyOnWriteArrayList;

import static io.joshworks.es2.Event.NO_VERSION;
import static java.util.Objects.requireNonNull;

public class SSTables {

    private static final String DATA_EXT = "sst";
    private static final String INDEX_EXT = "idx";
    private final Path folder;
    private final CopyOnWriteArrayList<SSTable> entries = new CopyOnWriteArrayList<>();

    public SSTables(Path folder) {
        this.folder = folder;
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
        for (SSTable sstable : entries) {
            var ie = sstable.get(stream, fromVersionInclusive);
            if (ie != null) {
                return ie;
            }
        }
        return null;
    }

    public int version(long stream) {
        for (SSTable sstable : entries) {
            int version = sstable.version(stream);
            if (version > NO_VERSION) {
                return version;
            }
        }
        return NO_VERSION;
    }


    public void flush(Iterator<ByteBuffer> iterator) {


    }



}
