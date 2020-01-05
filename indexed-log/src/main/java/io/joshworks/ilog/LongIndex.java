package io.joshworks.ilog;

import java.io.File;
import java.nio.ByteBuffer;

public class LongIndex extends Index {

    public LongIndex(File file, int size) {
        super(file, size, Long.BYTES);
    }

    @Override
    protected int compare(int idx, ByteBuffer key) {
        long key1 = mf.buffer().getLong(idx);
        long key2 = key.getLong(key.position());
        return Long.compare(key1, key2);
    }
}
