package io.joshworks.ilog.index;

import java.nio.ByteBuffer;

class IntRowKey implements RowKey {

    @Override
    public int compare(ByteBuffer k1, int k1Idx, ByteBuffer k2, int k2Idx) {
        return Integer.compare(k1.getInt(k1Idx), k2.getInt(k2Idx));
    }

    @Override
    public int keySize() {
        return Integer.BYTES;
    }
}
