//package io.joshworks.ilog.record;
//
//import io.joshworks.fstore.core.codec.Codec;
//import io.joshworks.fstore.core.io.buffers.Buffers;
//import io.joshworks.ilog.Record;
//import io.joshworks.ilog.index.IndexFunctions;
//import io.joshworks.ilog.index.RowKey;
//
//import java.nio.ByteBuffer;
//import java.util.List;
//
///**
// * -------- HEADER ---------
// * UNCOMPRESSED_SIZE (4bytes)
// * COMPRESSED_SIZE (4bytes)
// * ENTRY_COUNT (4bytes)
// * <p>
// * ------- KEYS REGION -----
// * KEY_ENTRY [OFFSET,KEY]
// * ...
// * -------- COMPRESSED VALUES REGION --------
// * COMPRESSED_BLOCK
// * ...
// */
//public class BlockRecords extends Records {
//
//    private static final int HEADER_BYTES = Integer.BYTES * 3;
//    //relative to block start
//    private static final int UNCOMPRESSED_SIZE_OFFSET = 0;
//    private static final int COMPRESSED_SIZE_OFFSET = Integer.BYTES;
//    private static final int ENTRY_COUNT_OFFSET = COMPRESSED_SIZE_OFFSET + Integer.BYTES;
//    private static final int KEY_REGION_OFFSET = ENTRY_COUNT_OFFSET + Integer.BYTES;
//
//
//    private final Codec codec;
//    private final int maxSize;
//    private ByteBuffer keyRegion;
//
//    //TODO pool needs to be properly configured
//    BlockRecords(String cachePoolName, RowKey rowKey, int maxItems, StripedBufferPool pool, Codec codec, int maxSize) {
//        super(cachePoolName, rowKey, maxItems, pool);
//        this.codec = codec;
//        this.maxSize = maxSize;
//    }
//
//    @Override
//    public int read(ByteBuffer data) {
//        if (!Record.isValid(data)) {
//            throw new RuntimeException("Invalid record");
//        }
//        int recStart = data.position();
//        int recSize = data.getInt(recStart);
//        int blockStart = data.getInt(recStart + Record2.KEY_OFFSET + rowKey.keySize());
//        int blockSize = data.getInt(recStart + Record2.VALUE_LEN_OFFSET);
//        int blockEnd = blockStart + blockSize;
//
//        int uncompressedSize = data.getInt(blockStart + UNCOMPRESSED_SIZE_OFFSET);
//        int compressedSize = data.getInt(blockStart + COMPRESSED_SIZE_OFFSET);
//        int entries = data.getInt(blockStart + ENTRY_COUNT_OFFSET);
//
//
//        this.keyRegion = readKeyRegion(data, entries);
//
//        ByteBuffer uncompressed = pool.allocate(uncompressedSize);
//
//
//        int plim = data.limit();
//
//        data.limit(blockEnd).position(blockStart);
//
//        codec.decompress(data, uncompressed);
//        uncompressed.flip();
//        //TODO add validation
//
//        super.read(uncompressed);
//
//        data.limit(plim).position(blockEnd);
//
//        return recSize;
//    }
//
//    private ByteBuffer readKeyRegion(ByteBuffer data, int entries) {
//        int keyRegionSize = entries * (Integer.BYTES + rowKey.keySize());
//        int keyRegionStart = data.position() + KEY_REGION_OFFSET;
//        ByteBuffer keyRegion = pool.allocate(keyRegionSize);
//
//        Buffers.copy(data, keyRegionStart, keyRegionSize, keyRegion);
//        keyRegion.flip();
//
//        return keyRegion;
//    }
//
//    public int find(ByteBuffer key, ByteBuffer dst, IndexFunctions fn) {
//        int cmp = indexedBinarySearch(key);
//        int idx = fn.apply(cmp);
//        if (idx < 0) {
//            return 0;
//        }
//
//        return read(idx, dst);
//    }
//
//    private int keyRegionEntrySize() {
//        return Integer.BYTES + rowKey.keySize();
//    }
//
//    private int indexedBinarySearch(ByteBuffer key) {
//        int low = 0;
//        int high = size();
//
//        while (low <= high) {
//            int mid = (low + high) >>> 1;
//
//            int keyPos = (mid * keyRegionEntrySize()) + Integer.BYTES; //Integer.BYTES skips the offset
//            int cmp = rowKey.compare(key, keyPos, key, 0);
//
//            if (cmp < 0)
//                low = mid + 1;
//            else if (cmp > 0)
//                high = mid - 1;
//            else
//                return mid; // key found
//        }
//        return -(low + 1);  // key not found
//    }
//
//    public int read(int idx, ByteBuffer dst) {
//        if (!readable()) {
//            throw new IllegalStateException();
//        }
//        if (idx < 0 || idx >= entries) {
//            throw new IndexOutOfBoundsException(idx);
//        }
//        if (!decompressed()) {
//            decompress();
//        }
//
//        int offset = keys.get(idx).offset;
//        data.position(offset);
//        return Record.copyTo(data, dst);
//    }
//
//    @Override
//    public void close() {
//        super.close();
//        if (keyRegion != null) {
//            pool.free(keyRegion);
//        }
//    }
//}
