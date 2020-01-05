package io.joshworks.ilog;

import java.io.File;
import java.nio.ByteBuffer;

public class LongIndex extends Index {

    public LongIndex(File file, int size) {
        super(file, size, Long.BYTES);
    }

    @Override
    protected int compare(ByteBuffer k1, int idx) {
        long key1 = k1.getLong(k1.position());
        long key2 = mf.getLong(idx);
        return Long.compare(key1, key2);
    }
}
