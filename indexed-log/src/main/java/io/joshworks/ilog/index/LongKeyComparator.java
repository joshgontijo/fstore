package io.joshworks.ilog.index;

import java.nio.ByteBuffer;

class LongKeyComparator implements KeyComparator {

    @Override
    public int compare(ByteBuffer k1, ByteBuffer k2) {
        return Long.compare(k1.getLong(k1.position()), k2.getLong(k2.position()));
    }

    @Override
    public int keySize() {
        return Long.BYTES;
    }
}
