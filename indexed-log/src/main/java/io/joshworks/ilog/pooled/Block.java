//package io.joshworks.ilog.pooled;
//
//import io.joshworks.fstore.core.codec.Codec;
//import io.joshworks.fstore.core.io.buffers.Buffers;
//import io.joshworks.ilog.Record;
//import io.joshworks.ilog.RecordBatch;
//import io.joshworks.ilog.index.KeyComparator;
//
//import java.nio.ByteBuffer;
//import java.util.ArrayList;
//import java.util.List;
//
///**
// * -------- HEADER ---------
// * UNCOMPRESSED_SIZE (4bytes)
// * ENTRY_COUNT (4bytes)
// * <p>
// * ------- KEYS REGION -----
// * KEY_1 (N bytes)
// * ENTRY_1_OFFSET (4bytes)
// * KEY_2 (N bytes)
// * ENTRY_2_OFFSET (4bytes)
// * ...
// * -------- COMPRESSED VALUES REGION --------
// * COMPRESSED_BLOB
// * ...
// */
//public class Block extends Pooled {
//
//    private final KeyComparator comparator;
//    private final boolean direct;
//    private final Codec codec;
//
//    private int uncompressedSize;
//    private int entries;
//
//    private final List<Key> keys = new ArrayList<>();
//    private final ByteBuffer compressedData;
//    private final ByteBuffer blockData;
//
//    Block(ObjectPool.Pool<? extends Pooled> pool, int blockSize, KeyComparator comparator, boolean direct, Codec codec) {
//        super(pool, blockSize, direct);
//        this.comparator = comparator;
//        this.direct = direct;
//        this.codec = codec;
//        this.compressedData = Buffers.allocate(blockSize, direct);
//        this.blockData = Buffers.allocate(blockSize, direct);
//    }
//
//    public void from(ByteBuffer block) {
//        uncompressedSize = block.getInt();
//        entries = block.getInt();
//
//        int keySize = comparator.keySize();
//
//        for (int i = 0; i < entries; i++) {
//            Key key = getOrAllocate(i);
//            key.offset = data.getInt();
//            Buffers.copy(block, block.position(), keySize, key.data);
//            Buffers.offsetPosition(block, keySize);
//        }
//
//        int copied = Buffers.copy(block, compressedData);
//        Buffers.offsetPosition(block, copied);
//    }
//
//    public void createFrom(ByteBuffer blockRecords) {
//        uncompressedSize = blockRecords.remaining();
//
//        int ppos = blockRecords.position();
//        int i = 0;
//        while (RecordBatch.hasNext(blockRecords)) {
//            Key key = getOrAllocate(i);
//
//            key.offset = blockRecords.position();
//            Record.KEY.copyTo(blockRecords, key.data);
//            RecordBatch.advance(blockRecords);
//        }
//        blockRecords.position(ppos);
//
//        codec.compress(blockRecords, compressedData);
//        compressedData.flip();
//    }
//
//    private Key getOrAllocate(int idx) {
//        while (idx >= keys.size()) {
//            keys.add(new Key());
//        }
//        return keys.get(idx);
//    }
//
//    public void decompress() {
//        if (decompressed()) {
//            return;
//        }
//        blockData.clear();
//        codec.decompress(compressedData, blockData);
//        blockData.flip();
//    }
//
//    public int binarySearch(ByteBuffer key, ByteBuffer dst) {
//        int keySize = comparator.keySize();
//        throw new UnsupportedOperationException();
//    }
//
//    public int get(int idx, ByteBuffer dst) {
//        if (!decompressed()) {
//            decompress();
//        }
//        int keySize = comparator.keySize();
//        throw new UnsupportedOperationException();
//    }
//
//
//    public boolean decompressed() {
//        throw new UnsupportedOperationException("TODO");
//    }
//
//    public boolean valid() {
//        throw new UnsupportedOperationException("TODO");
//    }
//
//    public int uncompressedSize() {
//        return data.getInt(0);
//    }
//
//    public int entryCount() {
//        return entries;
//    }
//
//    public ByteBuffer buffer() {
//        return data;
//    }
//
//    private class Key {
//        private int offset;
//        private ByteBuffer data = Buffers.allocate(comparator.keySize(), direct);
//    }
//
//}
