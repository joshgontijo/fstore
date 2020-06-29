package io.joshworks.ilog.pooled;

import io.joshworks.fstore.core.codec.Codec;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.ilog.Record;
import io.joshworks.ilog.index.IndexFunctions;
import io.joshworks.ilog.index.KeyComparator;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * -------- HEADER ---------
 * UNCOMPRESSED_SIZE (4bytes)
 * COMPRESSED_SIZE (4bytes)
 * ENTRY_COUNT (4bytes)
 * <p>
 * ------- KEYS REGION -----
 * KEY_ENTRY [OFFSET,KEY]
 * ...
 * -------- COMPRESSED VALUES REGION --------
 * COMPRESSED_BLOCK
 * ...
 */
public class HeapBlock extends Pooled {

    private final KeyComparator comparator;
    private final boolean direct;
    private final Codec codec;
    private State state = State.EMPTY;

    private static final int HEADER_BYTES = Integer.BYTES * 3;

    private int uncompressedSize;
    private int compressedSize;
    private int entries;

    private final List<Key> keys = new ArrayList<>();
    private final ByteBuffer block;

    public HeapBlock(ObjectPool<? extends Pooled> pool, int blockSize, KeyComparator comparator, boolean direct, Codec codec) {
        super(pool, blockSize, direct);
        this.comparator = comparator;
        this.direct = direct;
        this.codec = codec;
        this.block = Buffers.allocate(blockSize, direct);
    }

    private boolean hasCapacity(int entrySize) {
        return data.remaining() >=
                Record.HEADER_BYTES +
                        keyOverhead() +
                        HEADER_BYTES +
                        (entrySize + keyOverhead()) +
                        keyRegionSize();
    }

    private int keyRegionSize() {
        return entries * keyOverhead(); //offset
    }

    private int keyOverhead() {
        return comparator.keySize() + Integer.BYTES;
    }

    public boolean add(ByteBuffer srcRecord, int recStart, int count) {
        //TODO set max uncompressed data size, resize data ByteBuffer
        if (!State.EMPTY.equals(state) && !State.CREATING.equals(state)) {
            throw new IllegalStateException();
        }
        if (entries == 0) {
            //TODO implement all transition
            state = State.CREATING;
            data.clear();
        }

        if (!hasCapacity(count)) {
            return false;
        }

        //uses data as temporary storage
        //TODO check for readonly blocks
        Key key = getOrAllocate(entries);
        key.offset = data.position();

        int kOffset = recStart + Record.KEY.offset(null);
        int keySize = key.write(srcRecord, kOffset);
        assert keySize == comparator.keySize();

        int copied = Buffers.copy(srcRecord, recStart, count, data);
        entries++;
        return true;
    }

    public void from(ByteBuffer block, boolean decompress) {
        if (!State.EMPTY.equals(state)) {
            throw new IllegalStateException();
        }
        uncompressedSize = block.getInt();
        compressedSize = block.getInt();
        entries = block.getInt();

        for (int i = 0; i < entries; i++) {
            Key key = getOrAllocate(i);
            key.from(block);
        }

        //----- READ DECOMPRESSED ----
        if (decompress) {
            codec.decompress(block, data);
            data.flip();
            assert data.remaining() == uncompressedSize;
            state = State.DECOMPRESSED;
            return;
        }

        //----- READ COMPRESSED ----
        int copied = Buffers.copy(block, this.block);
        assert copied == uncompressedSize;

        this.block.flip();
        Buffers.offsetPosition(block, copied);

        state = State.COMPRESSED;

    }

    private Key getOrAllocate(int idx) {
        while (idx >= keys.size()) {
            keys.add(new Key(comparator, direct));
        }
        Key key = keys.get(idx);
        key.clear();
        return key;
    }

    public void compress() {
        if (!State.CREATING.equals(state)) {
            throw new IllegalStateException("Cannot compress block: Invalid block state: " + state);
        }

        data.flip();
        uncompressedSize = data.remaining();
        codec.compress(data, block);
        block.flip();
        compressedSize = block.remaining();

        state = State.COMPRESSED;
    }

    public void write(Consumer<ByteBuffer> writer) {
        if (!State.CREATING.equals(state)) {
            throw new IllegalStateException("Cannot compress block: Invalid block state: " + state);
        }

        block.clear();
        data.flip();

        int uncompressedSize = data.remaining();


        //------------ COMPRESSED DATA
        int keysSectionSize = keyOverhead() * entryCount();
        int dataRegion = HEADER_BYTES + keysSectionSize;
        block.position(dataRegion);
        codec.compress(data, block);

        int blockEnd = block.position();
        int compressedSize = blockEnd - dataRegion;

        //------------ BLOCK START
        block.position(0);
        block.putInt(uncompressedSize);
        block.putInt(compressedSize);
        block.putInt(entries);

        //------------ UNCOMPRESSED KEYS
        for (int i = 0; i < entries; i++) {
            Key key = keys.get(i);
            block.putInt(key.offset);
            Buffers.copy(key.data, block);
        }

        assert block.position() == dataRegion;
        block.limit(blockEnd).position(0);
        state = State.COMPRESSED;

        byte attribute = 0;
        Key firstKey = keys.get(0);
        data.clear();
        int recLen = Record.create(firstKey.data, block, data, attribute);

        data.flip();
        assert Record.isValid(data);
        writer.accept(data);
    }

    private int blockSize() {
        if (!State.COMPRESSED.equals(state)) {
            throw new IllegalStateException();
        }
        return HEADER_BYTES + keyRegionSize() + compressedSize;
    }

    @Override
    public void close() {
        clear();
        super.close();
    }

    public void clear() {
        data.clear();
        block.clear();
        uncompressedSize = 0;
        entries = 0;
        state = State.EMPTY;
    }

    public void decompress() {
        if (decompressed()) {
            return;
        }
        data.clear();
        codec.decompress(block, data);
        data.flip();

        assert uncompressedSize == data.remaining();

        state = State.DECOMPRESSED;
    }

    public int find(ByteBuffer key, ByteBuffer dst, IndexFunctions fn) {
        int cmp = indexedBinarySearch(keys, key);
        int idx = fn.apply(cmp);
        if (idx < 0) {
            return 0;
        }

        return read(idx, dst);
    }

    private int indexedBinarySearch(List<? extends Comparable<? super ByteBuffer>> list, ByteBuffer key) {
        int low = 0;
        int high = entries;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            Comparable<? super ByteBuffer> midVal = list.get(mid);
            int cmp = midVal.compareTo(key);

            if (cmp < 0)
                low = mid + 1;
            else if (cmp > 0)
                high = mid - 1;
            else
                return mid; // key found
        }
        return -(low + 1);  // key not found
    }

    public int read(int idx, ByteBuffer dst) {
        if (!readable()) {
            throw new IllegalStateException();
        }
        if (idx < 0 || idx >= entries) {
            throw new IndexOutOfBoundsException(idx);
        }
        if (!decompressed()) {
            decompress();
        }

        int offset = keys.get(idx).offset;
        data.position(offset);
        return Record.copyTo(data, dst);
    }


    public boolean decompressed() {
        return State.DECOMPRESSED.equals(state);
    }

    public boolean readable() {
        return decompressed() || State.COMPRESSED.equals(state);
    }

    public int uncompressedSize() {
        return data.getInt(0);
    }

    public int entryCount() {
        return entries;
    }

    public ByteBuffer buffer() {
        return data;
    }

    private static class Key implements Comparable<ByteBuffer> {
        private int offset;
        private final ByteBuffer data;
        private final KeyComparator comparator;

        private Key(KeyComparator comparator, boolean direct) {
            this.data = Buffers.allocate(comparator.keySize(), direct);
            this.comparator = comparator;
        }

        private int from(ByteBuffer src) {
            data.clear();
            offset = src.getInt();
            int copied = Buffers.copy(src, src.position(), comparator.keySize(), data);
            assert copied == comparator.keySize();
            Buffers.offsetPosition(src, copied);
            data.flip();
            return copied;
        }

        private int write(ByteBuffer src, int srcOffset) {
            data.clear();
            int copied = Buffers.copy(src, srcOffset, comparator.keySize(), data);
            assert copied == comparator.keySize();
            data.flip();
            return copied;
        }

        private void clear() {
            data.clear();
            offset = -1;
        }

        @Override
        public int compareTo(ByteBuffer o) {
            return comparator.compare(data, o);
        }
    }

    private enum State {
        EMPTY, CREATING, COMPRESSED, DECOMPRESSED, READ_ONLY,
    }
}
