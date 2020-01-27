package io.joshworks.ilog.lsm;

import io.joshworks.ilog.IndexedSegment;
import io.joshworks.ilog.index.KeyComparator;

import java.io.File;
import java.nio.ByteBuffer;

public class SSTable extends IndexedSegment {

    private BloomFilter bf;

    public SSTable(File file, int indexSize, KeyComparator comparator) {
        super(file, indexSize, comparator);
    }

    public int get(ByteBuffer key, ByteBuffer dst) {

    }

}
