package io.joshworks.ilog.lsm;

import io.joshworks.fstore.core.codec.Codec;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.ilog.Record;
import io.joshworks.ilog.RecordBatch;
import io.joshworks.ilog.fields.ArrayField;
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
 * <p>
 * ------- OFFSETS REGION (ARRAY FIELD) -----
 * ENTRY_COUNT (4bytes)
 * ENTRY_SIZE (4bytes)
 * OFFSET_1 (4 bytes)
 * OFFSET_2 (4 bytes)
 * ...
 * ------- KEYS REGION (ARRAY FIELD) -----
 * ENTRY_COUNT (4bytes)
 * ENTRY_SIZE (4bytes)
 * KEY_ENTRY (N bytes)
 * KEY_ENTRY (N bytes)
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

    public static final ArrayField OFFSETS = ArrayField.after(KEY_SIZE, Integer.BYTES);
    //    public static final ArrayField KEY_REGION = ArrayField.after(OFFSETS, Integer.BYTES);
    public static final ArrayField KEYS = ArrayField.after(OFFSETS, Integer.BYTES);
    public static final BlobField COMPRESSED_REGION = BlobField.after(KEYS, COMPRESSED_SIZE::get);

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

        int offsets = writeOffsets(block, blockRecords);
        int entries = writeKeys(block, blockRecords);

        if (block.remaining() < blockRecords.remaining()) {
            //this is just to guarantee that when using no compression, data will fit
            //most of the time when compressing data the compressed data size will be less than the block remaining bytes
            throw new IllegalStateException("Block records exceed remaining block size");
        }

        int compressedStart = block.position();
        codec.compress(blockRecords, block);
        int compressedSize = block.position() - compressedStart;

        block.flip();
        Block.UNCOMPRESSED_SIZE.set(block, uncompressedSize);
        Block.COMPRESSED_SIZE.set(block, compressedSize);

        return entries;
    }


    private static int writeKeys(ByteBuffer block, ByteBuffer blockRecords) {
        int ppos = blockRecords.position();
        int plim = blockRecords.limit();
        int entries = 0;
        int i = 0;
        while (RecordBatch.hasNext(blockRecords)) {
            KEYS.add(block, Record.KEY, blockRecords, i++);
            RecordBatch.advance(blockRecords);
            entries++;
        }
        blockRecords.limit(plim).position(ppos);
        return entries;
    }

    private static int writeOffsets(ByteBuffer block, ByteBuffer blockRecords) {
        int ppos = blockRecords.position();
        int plim = blockRecords.limit();

        int entries = 0;
        int i = 0;
        while (RecordBatch.hasNext(blockRecords)) {
            //FIXME COMMENTED OUT
//            OFFSETS.add(block, Record.KEY, blockRecords.position(), i++);
            RecordBatch.advance(blockRecords);
            entries++;
        }

        blockRecords.limit(plim).position(ppos);
        return entries;
    }


    public static void decompress(ByteBuffer block, ByteBuffer dst, Codec codec) {

        int offset = Block.COMPRESSED_REGION.relativeOffset(block);
        int size = Block.COMPRESSED_REGION.len(block);

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

        Block.decompress(block, decompressedTmp, codec);
        decompressedTmp.flip();


        //test ----
        int i = RecordBatch.countRecords(decompressedTmp);
        System.out.println(i);

        //--------


        decompressedTmp.position(recordOffset);
        int recordLen = Record.sizeOf(decompressedTmp);
        decompressedTmp.position(recordOffset).limit(recordOffset + recordLen);

        if (!Record.isValid(decompressedTmp)) {
            System.out.println();
        }

        assert Record.isValid(decompressedTmp);
        return Record.copyTo(decompressedTmp, dst);
    }

    private static int binarySearch(ByteBuffer block, ByteBuffer key, IndexFunctions func, KeyComparator comparator) {
        int keyRegionStart = KEYS.relativeOffset(block);
        int keyRegionSize = KEYS.len(block);
        int entryJump = Key.sizeOf(block);

        int idx = BufferBinarySearch.binarySearch(key, block, keyRegionStart, keyRegionSize, entryJump, comparator);
        return func.apply(idx);
    }

    public static String printKeys(ByteBuffer block) {
        int entries = ENTRY_COUNT.get(block);
        int keyRegionStart = KEYS.relativeOffset(block);
        int entryJump = Key.sizeOf(block);
        int keySize = Block.KEY_SIZE.get(block);

        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < entries; i++) {
            int pos = keyRegionStart + (i * entryJump);

            String k = "[BINARY]";
            if (keySize == Long.BYTES) {
                k = "" + block.getLong(pos);
            } else if (keySize == Integer.BYTES) {
                k = "" + block.getInt(pos);
            } else if (keySize == Short.BYTES) {
                k = "" + block.getShort(pos);
            } else if (keySize == Byte.BYTES) {
                k = "" + block.get(pos);
            }
            builder.append(k);

            int offset = Key.readOffset(block, i);
            builder.append(" => ")
                    .append(offset)
                    .append(System.lineSeparator());

        }
        return builder.toString();
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
            return KEY.len(block) + OFFSET.len(block);
        }

        private static int readOffset(ByteBuffer block, int keyIdx) {
            int baseOffset = Block.KEYS.relativeOffset(block);
            int keySize = Block.KEY_SIZE.get(block);
            int relativeKeyOffset = sizeOf(block) * keyIdx;
            return block.getInt(baseOffset + relativeKeyOffset + keySize);
        }


    }


}
