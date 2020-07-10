package io.joshworks.ilog.index;

import io.joshworks.ilog.record.Record2;

import java.nio.ByteBuffer;

class LongRowKey implements RowKey {

    @Override
    public int compare(ByteBuffer k1, ByteBuffer k2) {
        return compare(k1, k1.position(), k2, k2.position());
    }

    @Override
    public int compare(ByteBuffer k1, int k1Idx, ByteBuffer k2, int k2Idx) {
        return Long.compare(k1.getLong(k1Idx), k2.getLong(k2Idx));
    }

    @Override
    public int compare(Record2 rec1, Record2 rec2) {
        return 0;
    }

    @Override
    public int keySize() {
        return Long.BYTES;
    }
}
