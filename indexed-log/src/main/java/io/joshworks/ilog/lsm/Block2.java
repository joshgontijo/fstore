package io.joshworks.ilog.lsm;

import io.joshworks.fstore.core.codec.Codec;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.ilog.Record2;
import io.joshworks.ilog.index.IndexFunctions;
import io.joshworks.ilog.index.KeyComparator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

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
 * RECORD_1_COMPRESSED {@link Block2.Record}
 * RECORD_2_COMPRESSED {@link Block2.Record}
 * ...
 */
public class Block2 {

    private static final int UNCOMPRESSED_SIZE_LEN = Integer.BYTES;
    private static final int ENTRY_COUNT_LEN = Integer.BYTES;

    private static final int ENTRY_OFFSET_LEN = Integer.BYTES;

    private static final int UNCOMPRESSED_SIZE_OFFSET = 0;
    private static final int ENTRY_COUNT_OFFSET = UNCOMPRESSED_SIZE_OFFSET + UNCOMPRESSED_SIZE_LEN;
    private static final int KEY_REGION_OFFSET = ENTRY_COUNT_OFFSET + ENTRY_COUNT_LEN;

    public static final int HEADER_SIZE = UNCOMPRESSED_SIZE_LEN + ENTRY_COUNT_LEN;

    private static final int KEY_REGION_FIELDS_OVERHEAD = Integer.BYTES;

    public static int keyOverhead(int keySize) {
        return keySize + KEY_REGION_FIELDS_OVERHEAD;
    }

    public static int entryOffset(ByteBuffer record, int idx, int keySize) {
        int pos = keyPos(record, idx, keySize);
        return record.getInt(pos + keySize);
    }

    public static int writeKey(ByteBuffer record, ByteBuffer dst, int keyIdx, int keySize) {
        int pos = keyPos(record, keyIdx, keySize);
        return Buffers.copy(record, pos, keySize, dst);
    }

//    public static int entryLen(ByteBuffer record, int idx, int keySize) {
//        int pos = keyPos(record, idx, keySize);
//        return record.getInt(pos + keySize + Integer.BYTES);
//    }

    private static int keyPos(ByteBuffer record, int idx, int keySize) {
        int blockStart = Record2.VALUE.offset(record);

        int entries = record.getInt(blockStart + ENTRY_COUNT_OFFSET);
        int keyRegionStart = blockStart + KEY_REGION_OFFSET;
        if (idx < 0 || idx >= entries) {
            throw new IndexOutOfBoundsException();
        }
        return keyRegionStart + (idx * (keySize + Integer.BYTES));
    }

    public static void writeHeader(ByteBuffer block, int uncompressedSize, int entries) {
        block.putInt(relativePosition(block, UNCOMPRESSED_SIZE_OFFSET), uncompressedSize);
        block.putInt(relativePosition(block, ENTRY_COUNT_OFFSET), entries);
    }

    public static void compress(ByteBuffer src, ByteBuffer dst, Codec codec) {
        codec.compress(src, dst);
    }

    public static void decompress(ByteBuffer record, ByteBuffer dst, Codec codec) {
        int keySize = Record2.KEY.len(record);
        int compRegOffset = compressedRegionOffset(record, keySize);
        int compRegSize = uncompressedRegionSize(record);

        int plim = record.limit();
        int ppos = record.position();

        Buffers.view(record, compRegOffset, compRegSize);
        codec.decompress(record, dst);
        record.limit(plim).position(ppos);
    }

    public static int binarySearch(ByteBuffer record, ByteBuffer key, IndexFunctions func, KeyComparator keyComparator) {
        int blockStart = blockStart(record);

        int entries = record.getInt(blockStart + ENTRY_COUNT_OFFSET);
        int keyRegionStart = blockStart + KEY_REGION_OFFSET;
        int keySkipBytes = keyComparator.keySize() + KEY_REGION_FIELDS_OVERHEAD;
        int keyRegionSize = entries * keySkipBytes;

        int idx = BufferBinarySearch.binarySearch(key, record, keyRegionStart, keyRegionSize, keySkipBytes, keyComparator);
        return func.apply(idx);
    }

    private static int blockStart(ByteBuffer record) {
        return Record2.VALUE.offset(record);
    }

    private static int uncompressedRegionSize(ByteBuffer record) {
        int keySize = Record2.KEY.len(record);
        int entries = entries(record);
        int totalBlockSize = Record2.VALUE.len(record);
        return totalBlockSize - HEADER_SIZE - entries * keyOverhead(keySize);
    }

    private static int compressedRegionOffset(ByteBuffer record, int keySize) {
        int blockStart = Record2.VALUE.offset(record);
        int entries = record.getInt(blockStart + ENTRY_COUNT_OFFSET);
        return blockStart + HEADER_SIZE + (keyOverhead(keySize) * entries);
    }

    public static int maxEntrySize(int blockSize, int keySize) {
        int overheadPerKey = keySize + ENTRY_OFFSET_LEN;
        return blockSize - overheadPerKey;
    }

    public static int entries(ByteBuffer record) {
        int blockStart = Record2.VALUE.offset(record);
        return record.getInt(blockStart + ENTRY_COUNT_OFFSET);
    }

    public static int computeKeyOverhead(int keyLen) {
        return keyLen + KEY_REGION_FIELDS_OVERHEAD;
    }

    /**
     * VALUE_LEN (4 BYTES)
     * TIMESTAMP (8 BYTES)
     * ATTR (1 BYTES)
     * <p>
     * [VALUE] (N BYTES)
     */
    public static class Record {


        private static final int VALUE_LEN_LEN = Integer.BYTES;
        private static final int TIMESTAMP_LEN = Long.BYTES;
        private static final int ATTRIBUTES_LEN = Byte.BYTES;


        private static final int VALUE_LEN_OFFSET = 0;
        private static final int TIMESTAMP_OFFSET = VALUE_LEN_OFFSET + VALUE_LEN_LEN;
        private static final int ATTRIBUTES_OFFSET = TIMESTAMP_OFFSET + TIMESTAMP_LEN;
        private static final int VALUE_OFFSET = ATTRIBUTES_OFFSET + ATTRIBUTES_LEN;

        private static final int VALUE_REGION_ENTRY_OVERHEAD = Integer.BYTES + Long.BYTES + Byte.BYTES;

        public static int valueOverhead(int valueSize) {
            return valueSize + VALUE_REGION_ENTRY_OVERHEAD;
        }

        public static void fromRecord(ByteBuffer record, ByteBuffer blockRecords) {
            Record2.VALUE_LEN.copyTo(record, blockRecords);
            Record2.TIMESTAMP.copyTo(record, blockRecords);
            Record2.ATTRIBUTE.copyTo(record, blockRecords);
            Record2.VALUE.copyTo(record, blockRecords);
        }

        public static int computedSize(int valueLen) {
            return VALUE_LEN_LEN + TIMESTAMP_LEN + ATTRIBUTES_LEN + valueLen;
        }

        public static void writeValue(ByteBuffer decompressedBlock, int valueOffset, int entryLen, ByteBuffer dst) {
            Buffers.copy(decompressedBlock, VALUE_REGION_ENTRY_OVERHEAD + valueOffset, entryLen, dst);
        }

        public static int valueSize(ByteBuffer data) {
            return data.getInt(relativePosition(data, VALUE_LEN_OFFSET));
        }

        public static long timestamp(ByteBuffer data) {
            return data.getLong(relativePosition(data, TIMESTAMP_OFFSET));
        }

        public static boolean hasAttribute(ByteBuffer data, int attribute) {
            byte attr = attribute(data);
            return (attr & (1 << attribute)) == 1;
        }

        public static byte attribute(ByteBuffer data) {
            return data.get(relativePosition(data, ATTRIBUTES_OFFSET));
        }

        public static int writeTo(ByteBuffer record, WritableByteChannel channel) throws IOException {
            int rsize = valueSize(record);
            if (record.remaining() < rsize) {
                return 0;
            }
            int plimit = record.limit();
            record.limit(record.position() + rsize);
            int written = channel.write(record);
            record.limit(plimit);
            return written;
        }

    }

}
