package io.joshworks.ilog.lsm;

import io.joshworks.ilog.IndexedSegment;
import io.joshworks.ilog.index.KeyComparator;

import java.io.File;

public class SSTable extends IndexedSegment {

    public SSTable(File file, int indexSize, KeyComparator comparator) {
        super(file, indexSize, comparator);
    }

}
