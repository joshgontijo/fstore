package io.joshworks.es;

import io.joshworks.ilog.index.RowKey;

import java.nio.ByteBuffer;

public class StreamKey implements RowKey {

    @Override
    public int compare(ByteBuffer k1, int k1Idx, ByteBuffer k2, int k2Idx) {
        long streamId1 = k1.getLong(k1Idx);
        long streamId2 = k2.getLong(k2Idx);
        int streamIdCompare = Long.compare(streamId1, streamId2);
        if (streamIdCompare != 0) {
            return streamIdCompare;
        }

        int s1version = k1.getInt(k1Idx + Long.BYTES);
        int s2version = k2.getInt(k2Idx + Long.BYTES);
        return Integer.compare(s1version, s2version);
    }

    @Override
    public int keySize() {
        return Long.BYTES + Integer.BYTES;
    }
}
