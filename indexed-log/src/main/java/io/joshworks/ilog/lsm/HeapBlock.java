package io.joshworks.ilog.lsm;

import io.joshworks.fstore.core.codec.Codec;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.ilog.Record;
import io.joshworks.ilog.RecordBatch;
import io.joshworks.ilog.index.IndexFunctions;
import io.joshworks.ilog.index.KeyComparator;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * -------- HEADER ---------
 * UNCOMPRESSED_SIZE (4bytes)
 * COMPRESSED_SIZE (4bytes)
 * ENTRY_COUNT (4bytes)
 * KEY_SIZE (4bytes)
 * <p>
 * ------- KEYS REGION -----
 * KEY_ENTRY {@link Block.Key}
 * KEY_ENTRY {@link Block.Key}
 * ...
 * -------- COMPRESSED VALUES REGION --------
 * RECORD_1_COMPRESSED {@link BlockRecord}
 * RECORD_2_COMPRESSED {@link BlockRecord}
 * ...
 */
public class HeapBlock {

    private int uncompressedSize;
    private int compressedSize;
    private int entries;
    private final int keySize;

    private static final ThreadLocal<Key> searchKey = ThreadLocal.withInitial(() -> new Key(null));

    private final List<Key> keys = new ArrayList<>();
    private final ByteBuffer compressedBlock;

    public HeapBlock(int keySize, int blockSize) {
        this.keySize = keySize;
        this.compressedBlock = Buffers.allocate(blockSize, false);
    }

    public void readFrom(ByteBuffer blockData, int blockStart) {
        compressedBlock.clear();

        this.uncompressedSize = blockData.getInt(blockStart);
        this.compressedSize = blockData.getInt(blockStart + 4);
        this.entries = blockData.getInt(blockStart + 8);
        int keySize = blockData.getInt(blockStart + 12);
        assert keySize == this.keySize;

        int offset = blockStart + 16;
        for (int i = 0; i < entries; i++) {
            Key key = getKey(i);
            key.offset = blockData.getInt(offset);
            int copied = Buffers.copy(blockData, offset + 4, keySize, key.data);
            key.data.flip();
            assert copied == keySize;
            offset += 4 + keySize;
        }

        Buffers.copy(blockData, offset, compressedSize, compressedBlock);
        compressedBlock.flip();
        assert compressedBlock.remaining() == compressedSize;
    }

    public static void create(HeapBlock block, ByteBuffer blockRecords, Codec codec) {
        int keyIdx = 0;
        int uncompressedSize = blockRecords.remaining();
        block.compressedBlock.clear();

        int ppos = blockRecords.position();
        while (RecordBatch.hasNext(blockRecords)) {
            Key key = block.getKey(keyIdx++);
            key.data.clear();

            key.offset = blockRecords.position();
            Record.KEY.copyTo(blockRecords, key.data);
            key.data.flip();

            RecordBatch.advance(blockRecords);
        }
        blockRecords.position(ppos);

        codec.compress(blockRecords, block.compressedBlock);
        block.compressedBlock.flip();

        block.uncompressedSize = uncompressedSize;
        block.compressedSize = block.compressedBlock.remaining();
        block.entries = keyIdx;
    }

    public ByteBuffer first() {
        return getKey(0).data;
    }

    public void copyTo(ByteBuffer dst) {
        dst.putInt(uncompressedSize);
        dst.putInt(compressedSize);
        dst.putInt(entries);
        dst.putInt(keySize);

        for (int i = 0; i < entries; i++) {
            Key key = keys.get(i);
            dst.putInt(key.offset);
            Buffers.copy(key.data, dst);
        }
        Buffers.copy(compressedBlock, dst);
    }

    public int binarySearch(ByteBuffer key, KeyComparator comparator, IndexFunctions fn) {
        Key k = searchKey.get();
        k.data = key;
        int idx = Collections.binarySearch(keys, k, (o1, o2) -> comparator.compare(o1.data, o2.data));
        idx = fn.apply(idx);
        if (idx < 0) {
            return idx;
        }
        return keys.get(idx).offset;
    }

    public int decompress(ByteBuffer dst, Codec codec) {
        int dstPos = dst.position();
        codec.decompress(compressedBlock, dst);
        int diff = dst.position() - dstPos;
        assert uncompressedSize == diff;
        return diff;
    }

    private Key getKey(int idx) {
        if (idx >= keys.size()) {
            ByteBuffer keyData = Buffers.allocate(keySize, false);
            keys.add(idx, new Key(keyData));
        }
        return keys.get(idx);
    }

    public int uncompressedSize() {
        return uncompressedSize;
    }

    public int compressedSize() {
        return compressedSize;
    }

    public int entries() {
        return entries;
    }

    public int keySize() {
        return keySize;
    }

    private static class Key {
        private int offset;
        private ByteBuffer data;

        private Key(ByteBuffer data) {
            this.data = data;
        }

    }


}
