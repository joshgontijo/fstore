package io.joshworks.ilog.pooled;

import io.joshworks.ilog.index.KeyComparator;

import java.nio.ByteBuffer;

import static io.joshworks.fstore.core.io.buffers.Buffers.relativePosition;

/**
 * -------- HEADER ---------
 * UNCOMPRESSED_SIZE (4bytes)
 * ENTRY_COUNT (4bytes)
 * <p>
 * ------- KEYS REGION -----
 * KEY_1 (N bytes)
 * ENTRY_1_OFFSET (4bytes)
 * KEY_2 (N bytes)
 * ENTRY_2_OFFSET (4bytes)
 * ...
 * -------- COMPRESSED VALUES REGION --------
 * RECORD_1_COMPRESSED {@link CompressedBlockData}
 * RECORD_2_COMPRESSED {@link CompressedBlockData}
 * ...
 */
public class Block extends Pooled {

    private static final int KEYS_REGION_OFFSET = 8;
    private static final int VALUE_OFFSET_LEN = Integer.BYTES;

    private final int keySize;

    Block(ObjectPool.Pool<? extends Pooled> pool, int blockSize, int keySize, boolean direct) {
        super(pool, blockSize, direct);
        this.keySize = keySize;
    }

    public int uncompressedSize() {
        return data.getInt(0);
    }

    public int entryCount() {
        return data.getInt(4);
    }

    public ByteBuffer buffer() {
        return data;
    }

    public int entryOffset(int idx) {
        int offset = relativePosition(data, KEYS_REGION_OFFSET);
        int entryIdx = idx * (keySize + VALUE_OFFSET_LEN);
        return data.getInt(offset + entryIdx + keySize);
    }

    public int compareKey(ByteBuffer key, int idx, KeyComparator comparator) {
        int offset = relativePosition(data, KEYS_REGION_OFFSET);
        int entryIdx = idx * (keySize + VALUE_OFFSET_LEN);
        return comparator.compare(data, offset + entryIdx, key, key.position());
    }

    public int size() {
        return entryCount() * (keySize + VALUE_OFFSET_LEN);
    }

}
