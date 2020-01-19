package io.joshworks.ilog.index;

import java.nio.ByteBuffer;

class IntKeyComparator implements KeyComparator {
    @Override
    public int compare(ByteBuffer k1, ByteBuffer k2) {
        return Integer.compare(k1.getInt(), k2.getInt());
    }

    @Override
    public int keySize() {
        return Integer.BYTES;
    }
}
