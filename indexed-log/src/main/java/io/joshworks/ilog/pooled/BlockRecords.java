package io.joshworks.ilog.pooled;

import io.joshworks.fstore.core.io.buffers.Buffers;

import java.nio.ByteBuffer;

import static io.joshworks.fstore.core.io.buffers.Buffers.relativePosition;

/**
 * Array of decompressed records
 *
 * VALUE_LEN (4 BYTES)
 * TIMESTAMP (8 BYTES)
 * ATTR (1 BYTES)
 * <p>
 * [VALUE] (N BYTES)
 */
public class BlockRecords extends Pooled {

    private static final int HEADER_SIZE = Integer.BYTES + Long.BYTES + Byte.BYTES;

    BlockRecords(ObjectPool.Pool<? extends Pooled> pool, int size, boolean direct) {
        super(pool, size, direct);
    }

    public int entries() {

    }

    public int valueSize(int idx) {
        return data.get(relativePosition(data, 0));
    }

    private int nPos(int idx) {
        int pos = 0;
        for (int i = 0; i < idx; i++) {
            int valueSize = data.get(relativePosition(data, 0));
        }
    }

}
