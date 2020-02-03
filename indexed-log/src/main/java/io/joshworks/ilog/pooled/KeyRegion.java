package io.joshworks.ilog.pooled;

import java.nio.ByteBuffer;

public class KeyRegion {

    private static final int VALUE_OFFSET_LEN = Integer.BYTES;
    private final int keySize;

    ByteBuffer backingBuffer;

    public KeyRegion(int keySize) {
        this.keySize = keySize;
    }

    public int entries() {
        return count / (keySize + VALUE_OFFSET_LEN);
    }


}
