package io.joshworks.ilog.lsm;

import io.joshworks.fstore.core.codec.Codec;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.fstore.core.util.ByteBufferChecksum;
import io.joshworks.ilog.Record;
import io.joshworks.ilog.RecordBatch;
import io.joshworks.ilog.fields.BlobField;
import io.joshworks.ilog.fields.IntField;
import io.joshworks.ilog.index.IndexFunctions;
import io.joshworks.ilog.index.KeyComparator;
import io.joshworks.ilog.pooled.BlockRecord;

import java.nio.ByteBuffer;

/**
 * -------- HEADER ---------
 * UNCOMPRESSED_SIZE (4bytes)
 * COMPRESSED_SIZE (4bytes)
 * ENTRY_COUNT (4bytes)
 * KEY_SIZE (4bytes)
 * CHECKSUM (4bytes)
 * <p>
 * ------- KEYS REGION -----
 * KEY_ENTRY {@link Key}
 * KEY_ENTRY {@link Key}
 * ...
 * -------- COMPRESSED VALUES REGION --------
 * RECORD_1_COMPRESSED {@link BlockRecord}
 * RECORD_2_COMPRESSED {@link BlockRecord}
 * ...
 */
public class Block {

    public static final IntField UNCOMPRESSED_SIZE = new IntField(0);
    public static final IntField COMPRESSED_SIZE = IntField.after(UNCOMPRESSED_SIZE);
    public static final IntField ENTRY_COUNT = IntField.after(COMPRESSED_SIZE);
    public static final IntField KEY_SIZE = IntField.after(ENTRY_COUNT);
    public static final IntField CHECKSUM = IntField.after(KEY_SIZE);

    public static final BlobField KEY_REGION = BlobField.after(CHECKSUM, Block::keyRegionSize);
    public static final BlobField COMPRESSED_REGION = BlobField.after(KEY_REGION, COMPRESSED_SIZE::get);

    public static final int HEADER_SIZE = Integer.BYTES * 5;

    public static float compressionRatio(ByteBuffer block) {
        return ((float) UNCOMPRESSED_SIZE.get(block) - COMPRESSED_SIZE.get(block)) / 100;
    }

    //total capacity for keys + compressed entries
    public static int maxCapacity(ByteBuffer block) {
        return block.remaining() - HEADER_SIZE;
    }

    private static int keyRegionSize(ByteBuffer block) {
        return (KEY_SIZE.get(block) + Key.OVERHEAD) * ENTRY_COUNT.get(block);
    }

    public static int blockEntryOverhead(int keySize, int valueSize) {
        return (keySize + Key.OVERHEAD) + valueSize;
    }

    public static int create(ByteBuffer blockRecords, ByteBuffer block, int keySize, Codec codec) {
        block.clear(); //make sure all data is just for this block
        if (!blockRecords.hasRemaining()) {
            throw new IllegalStateException("Block records must be empty");
        }
        int capacity = maxCapacity(block);
        if (capacity < blockRecords.remaining()) {
            throw new IllegalArgumentException("Block record must no exceed block data capacity of " + capacity);
        }

        int uncompressedSize = blockRecords.remaining();

        int keyOffset = KEY_REGION.offset(block);
        block.position(keyOffset);
        int entries = writeKeys(block, blockRecords, keySize);

        if (block.remaining() < blockRecords.remaining()) {
            //this is just to guarantee that when using no compression, data will fit
            //most of the time when compressing data the compressed data size will be less than the block remaining bytes
            throw new IllegalStateException("Block records exceed remaining block size");
        }


        int compressedStart = block.position();
        codec.compress(blockRecords, block);
        int compressedSize = block.position() - compressedStart;

        int checksum = ByteBufferChecksum.crc32(block, compressedStart, compressedSize);

        block.flip();
        Block.UNCOMPRESSED_SIZE.set(block, uncompressedSize);
        Block.COMPRESSED_SIZE.set(block, compressedSize);
        Block.ENTRY_COUNT.set(block, entries);
        Block.KEY_SIZE.set(block, keySize);
        Block.CHECKSUM.set(block, checksum);

        return entries;
    }

    public static boolean isValid(ByteBuffer block) {
        if (block.remaining() < HEADER_SIZE) {
            return false;
        }
        int uncompressedSize = Block.UNCOMPRESSED_SIZE.get(block);
        int entries = Block.ENTRY_COUNT.get(block);
        int compressedRegionStart = Block.COMPRESSED_REGION.relativeOffset(block);
        int regionSize = Block.COMPRESSED_REGION.len(block);
        int checksum = Block.CHECKSUM.get(block);
        int keyRegionOffset = Block.KEY_REGION.offset(block);
        int keyRegionLen = Block.KEY_REGION.len(block);
        int keySize = Block.KEY_SIZE.len(block);

        if ((keySize + Key.OVERHEAD) * entries != keyRegionLen) {
            return false;
        }

        int computedChecksum = ByteBufferChecksum.crc32(block, compressedRegionStart, regionSize);
        return computedChecksum == checksum;
    }

    private static int writeKeys(ByteBuffer block, ByteBuffer blockRecords, int expectedKeySize) {
        int ppos = blockRecords.position();
        int plim = blockRecords.limit();

        int bppos = block.position();

        int entries = 0;
        while (RecordBatch.hasNext(blockRecords)) {
            block.putInt(blockRecords.position());
            int koff = Record.KEY.relativeOffset(blockRecords);
            int klen = Record.KEY.len(blockRecords);
            if (klen != expectedKeySize) {
                blockRecords.limit(plim).position(ppos);
                block.position(bppos);
                throw new IllegalStateException("Expected key of size " + expectedKeySize + ", got " + klen);
            }

            Buffers.copy(blockRecords, koff, klen, block);

            RecordBatch.advance(blockRecords);
            entries++;
        }

        blockRecords.limit(plim).position(ppos);
        return entries;
    }


    public static void decompress(ByteBuffer block, ByteBuffer dst, Codec codec) {
        int offset = Block.COMPRESSED_REGION.relativeOffset(block);
        int size = Block.COMPRESSED_REGION.len(block);

        assert isValid(block);

        int ppos = block.position();
        int plim = block.limit();

        Buffers.view(block, offset, size);
        codec.decompress(block, dst);

        block.limit(plim).position(ppos);
    }

    public static int read(ByteBuffer block,
                           ByteBuffer key,
                           ByteBuffer decompressedTmp,
                           ByteBuffer dst,
                           IndexFunctions func,
                           KeyComparator comparator,
                           Codec codec) {

        int keyIdx = Block.binarySearch(block, key, func, comparator);
        if (keyIdx < 0) {
            return 0;
        }

        int recordOffset = Key.readOffset(block, keyIdx);

        int uncompressedSize = Block.UNCOMPRESSED_SIZE.get(block);
        int entries = Block.ENTRY_COUNT.get(block);
        int offset = Block.COMPRESSED_REGION.offset(block);
        int regionSize = Block.COMPRESSED_REGION.len(block);
        int keyRegionOffset = Block.KEY_REGION.offset(block);
        int keyRegionLen = Block.KEY_REGION.len(block);

        Block.decompress(block, decompressedTmp, codec);
        decompressedTmp.flip();


        //test ----
        int i = RecordBatch.countRecords(decompressedTmp);
        System.out.println(i);

        //--------


        int recordLen = Record.sizeOf(decompressedTmp);
        decompressedTmp.position(recordOffset).limit(recordLen);

        assert Record.isValid(decompressedTmp);
        return Record.copyTo(decompressedTmp, dst);
    }

    private static int binarySearch(ByteBuffer block, ByteBuffer key, IndexFunctions func, KeyComparator comparator) {
        int entries = ENTRY_COUNT.get(block);
        int keyRegionStart = KEY_REGION.relativeOffset(block);
        int entryJump = Key.sizeOf(block);
        int keyRegionSize = entries * entryJump;

        int idx = BufferBinarySearch.binarySearch(key, block, keyRegionStart, keyRegionSize, entryJump, comparator);
        return func.apply(idx);
    }


    /**
     * ENTRY_1_OFFSET (4bytes)
     * KEY_1 (N bytes)
     */
    private static class Key {

        private static final IntField OFFSET = new IntField(0);
        private static final BlobField KEY = BlobField.after(OFFSET, Block.KEY_SIZE::get);

        private static final int OVERHEAD = Integer.BYTES;

        public static int sizeOf(ByteBuffer block) {
            return OFFSET.len(block) + KEY.len(block);
        }

        private static int readOffset(ByteBuffer block, int keyIdx) {
            int baseOffset = Block.KEY_REGION.offset(block);
            int relativeKeyOffset = sizeOf(block) * keyIdx;
            return block.getInt(baseOffset + relativeKeyOffset);
        }


    }


}
