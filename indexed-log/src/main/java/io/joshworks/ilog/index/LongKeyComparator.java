package io.joshworks.ilog.index;

import java.nio.ByteBuffer;

class LongKeyComparator implements KeyComparator {
    @Override
    public int compare(ByteBuffer k1, ByteBuffer k2) {
        return Long.compare(k1.getLong(), k2.getLong());
    }

    @Override
    public int keySize() {
        return Long.BYTES;
    }
}
