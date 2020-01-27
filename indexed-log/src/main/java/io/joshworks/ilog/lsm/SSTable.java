package io.joshworks.ilog.lsm;

import io.joshworks.ilog.IndexedSegment;
import io.joshworks.ilog.index.KeyComparator;

import java.io.File;
import java.nio.ByteBuffer;

public class SSTable extends IndexedSegment {

    public SSTable(File file, int indexSize, KeyComparator comparator) {
        super(file, indexSize, comparator);
    }

    @Override
    public long find(ByteBuffer key) {
        return super.find(key);
    }
}
