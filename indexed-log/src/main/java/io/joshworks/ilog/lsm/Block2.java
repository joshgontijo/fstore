package io.joshworks.ilog.lsm;

import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.codec.Codec;
import io.joshworks.ilog.index.KeyComparator;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static io.joshworks.fstore.core.io.buffers.Buffers.relativePosition;

/**
 * -------- HEADER ---------
 * UNCOMPRESSED_SIZE (4bytes)
 * ENTRY_COUNT (4bytes)
 * KEYS_REGION_OFFSET (4 bytes)
 * <p>
 * -------- BODY --------
 * ENTRY_1 (N Bytes)
 * ENTRY_2 (N Bytes)
 * ...
 * ------- KEYS REGION -----
 * KEY_1 (N bytes)
 * ENTRY_1_OFFSET (4bytes)
 * ENTRY_1_LEN (4bytes)
 * KEY_2 (N bytes)
 * ENTRY_2_OFFSET (4bytes)
 * ENTRY_2_LEN (4bytes)
 * ...
 */
public class Block2 implements Iterable<ByteBuffer> {

    private static final int UNCOMPRESSED_SIZE_LEN = Integer.BYTES;
    private static final int ENTRY_COUNT_LEN = Integer.BYTES;
    private static final int KEYS_REGION_OFFSET_LEN = Integer.BYTES;

    private static final int ENTRY_LEN_LEN = Integer.BYTES;
    private static final int ENTRY_OFFSET_LEN = Integer.BYTES;

    private static final int UNCOMPRESSED_SIZE_OFFSET = 0;
    private static final int ENTRY_COUNT_OFFSET = UNCOMPRESSED_SIZE_OFFSET + UNCOMPRESSED_SIZE_LEN;
    private static final int KEY_REGION_OFFSET = ENTRY_COUNT_OFFSET + ENTRY_COUNT_LEN;

    private static final int HEADER_SIZE = UNCOMPRESSED_SIZE_LEN + ENTRY_COUNT_LEN + KEYS_REGION_OFFSET_LEN;


    public static int add(ByteBuffer block, int keySize, ByteBuffer record) {
        int entries = block.getInt(relativePosition(block, ENTRY_COUNT_OFFSET));
        int keyEntryLen = keySize + ENTRY_LEN_LEN; //KEY_1 + ENTRY_1_LEN
        int currentSize = HEADER_SIZE + (entries * keyEntryLen) +


    }


    private static int keysRegionSize(ByteBuffer block) {
        int startPos = relativePosition(block, KEY_REGION_OFFSET);
    }

    public static int read(ByteBuffer block, int keyIdx, ByteBuffer dst) {
        return 0;
    }

    public static void decompress(ByteBuffer compressed, Codec codec, ByteBuffer dst) {
        //head header
        int entryCount = compressed.getInt();
        int uncompressedSize = compressed.getInt();

        //LZ4 required destination buffer to have the exact number uncompressed bytes
        codec.decompress(compressed, dst);
        dst.flip();

        for (int i = 0; i < entryCount; i++) {
            int entryLen = dst.getInt();
            lengths.add(entryLen);
            positions.add(dst.position());
            dst.position(dst.position() + entryLen);
        }
        if (lengths.size() != entryCount) {
            throw new IllegalStateException("Expected block with " + entryCount + ", got " + lengths.size());
        }
        return dst;
    }

    public static int binarySearch(ByteBuffer compressedBlock, ByteBuffer key, KeyComparator keyComparator) {
        int entries = compressedBlock.getInt(relativePosition(compressedBlock, ENTRY_COUNT_OFFSET));
        int keyRegionStart = compressedBlock.getInt(relativePosition(compressedBlock, KEY_REGION_OFFSET));
        int keyRegionSize = entries * keyComparator.keySize();

        return BufferBinarySearch.binarySearch(key, compressedBlock, keyRegionStart, keyRegionSize, keyComparator);
    }

    public static boolean hasRemaining(ByteBuffer block, ByteBuffer entry, int keys, int keySize) {
        int overheadPerKey = keySize + ENTRY_LEN_LEN + ENTRY_OFFSET_LEN;
        return block.remaining() >= entry.remaining() + (keys * overheadPerKey);
    }

    public static int maxEntrySize(int blockSize, int keySize) {
        int overheadPerKey = keySize + ENTRY_LEN_LEN + ENTRY_OFFSET_LEN;
        return blockSize - overheadPerKey;
    }


    protected ByteBuffer createBuffer(int size) {
        return ByteBuffer.allocate(size);
    }

    //returns true if added, false otherwise
    public boolean add(ByteBuffer entry) {
        if (!checkConstraints(entry)) {
            return false;
        }
        int entrySize = entry.remaining();

        data.putInt(entrySize);
        lengths.add(entrySize);
        positions.add(data.position());
        data.put(entry);
        return true;
    }

    boolean checkConstraints(ByteBuffer entry) {
        if (readOnly) {
            throw new IllegalStateException("Block is read only");
        }
        if (entry.remaining() == 0) {
            throw new IllegalArgumentException("Empty data is not allowed");
        }
        int entryWithHeaderSize = entry.remaining() + entryHeaderSize();
        if (entryWithHeaderSize > data.capacity()) {
            throw new IllegalArgumentException("Record [data=" + entry.remaining() + ", entryHeader=" + entryHeaderSize() + "] cannot be greater than (" + data.capacity() + ")");
        }
        return entryWithHeaderSize <= data.remaining();
    }

    void pack(Codec codec, ByteBuffer dst) {
        if (readOnly()) {
            throw new IllegalStateException("Block is read only");
        }
        if (isEmpty()) {
            return;
        }

        //block header
        writeBlockHeader(dst);

        //block data
        data.flip();
        writeBlockContent(codec, dst);
    }

    private void writeBlockContent(Codec codec, ByteBuffer dst) {
        codec.compress(data, dst);
    }

    protected void writeBlockHeader(ByteBuffer dst) {
        dst.putInt(entryCount());
        dst.putInt(uncompressedSize());
    }

    protected ByteBuffer unpack(Codec codec, ByteBuffer compressedBlock) {
        //head header
        int entryCount = compressedBlock.getInt();
        int uncompressedSize = compressedBlock.getInt();

        //LZ4 required destination buffer to have the exact number uncompressed bytes
        ByteBuffer data = createBuffer(uncompressedSize);
        codec.decompress(compressedBlock, data);
        data.flip();

        for (int i = 0; i < entryCount; i++) {
            int entryLen = data.getInt();
            lengths.add(entryLen);
            positions.add(data.position());
            data.position(data.position() + entryLen);
        }
        if (lengths.size() != entryCount) {
            throw new IllegalStateException("Expected block with " + entryCount + ", got " + lengths.size());
        }
        return data;
    }

    public void clear() {
        lengths.clear();
        positions.clear();
        data.clear();
    }

    public int entryCount() {
        return lengths.size();
    }

    public ByteBuffer first() {
        if (isEmpty()) {
            return null;
        }
        return get(0);
    }

    public ByteBuffer last() {
        if (isEmpty()) {
            return null;
        }
        return get(entryCount() - 1);
    }

    public ByteBuffer get(int idx) {
        if (isEmpty() || idx >= entryCount() || idx < 0) {
            return null;
        }
        int pos = positions.get(idx);
        int len = lengths.get(idx);
        return data.asReadOnlyBuffer()
                .limit(pos + len)
                .position(pos)
                .slice();
    }

    public boolean readOnly() {
        return readOnly;
    }

    public boolean isEmpty() {
        return lengths.isEmpty();
    }

    public int entryHeaderSize() {
        return Integer.BYTES;
    }

    public int blockHeaderSize() {
        return Integer.BYTES * 2; //entries + uncompressedSize
    }

    public int uncompressedSize() {
        return data.position();
    }

    public <T> List<T> deserialize(Serializer<T> serializer) {
        List<T> items = new ArrayList<>();
        for (int i = 0; i < lengths.size(); i++) {
            T item = serializer.fromBytes(get(i));
            items.add(item);
        }
        return items;
    }

    @Override
    public Iterator<ByteBuffer> iterator() {
        return new BlockEntryIterator(this);
    }

    public int remaining() {
        return data.remaining();
    }

    public int capacity() {
        return data.capacity();
    }

    public int position() {
        return data.position();
    }

    private static final class BlockEntryIterator implements Iterator<ByteBuffer> {

        private final Block2 block;
        private int idx;

        private BlockEntryIterator(Block2 block) {
            this.block = block;
        }

        public boolean hasNext() {
            return idx < block.entryCount();
        }

        public ByteBuffer next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            ByteBuffer found = block.get(idx);
            idx++;
            return found;
        }
    }
}
