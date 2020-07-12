package io.joshworks.ilog.index;

import java.nio.ByteBuffer;

class LongRowKey implements RowKey {

    @Override
    public int compare(ByteBuffer k1, int k1Idx, ByteBuffer k2, int k2Idx) {
        return Long.compare(k1.getLong(k1Idx), k2.getLong(k2Idx));
    }

    @Override
    public int keySize() {
        return Long.BYTES;
    }
}
