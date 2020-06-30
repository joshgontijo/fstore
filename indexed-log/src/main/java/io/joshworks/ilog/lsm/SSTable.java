package io.joshworks.ilog.lsm;

import io.joshworks.ilog.IndexedSegment;
import io.joshworks.ilog.index.RowKey;

import java.io.File;

public class SSTable extends IndexedSegment {

    public SSTable(File file, int indexSize, RowKey comparator) {
        super(file, indexSize, comparator);
    }

}
