package io.joshworks.ilog.pooled;

import java.nio.ByteBuffer;

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
 * RECORD_1_COMPRESSED {@link Block2.Record}
 * RECORD_2_COMPRESSED {@link Block2.Record}
 * ...
 */
public class Block extends Pooled {

    private final int keySize;

    private final CompressedBlockData compressedClockRef;
    private final KeyRegion keysRef;

    Block(ObjectPool.Pool<? extends Pooled> pool, int blockSize, int keySize, boolean direct) {
        super(pool, blockSize, direct);
        this.keySize = keySize;
        this.compressedClockRef = new CompressedBlockData();
        this.keysRef = new KeyRegion(keySize);
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

    public KeyRegion keyRegion() {
        keysRef.backingBuffer = data;
        keysRef.offset = 8;
        keysRef.count = entryCount() * keySize
    }

    public CompressedBlockData blockData() {
        compressedClockRef.backingBuffer = data;
        compressedClockRef.offset =
    }

}
