package io.joshworks.ilog.pooled;

import io.joshworks.fstore.core.codec.Codec;
import io.joshworks.fstore.core.io.buffers.Buffers;
import io.joshworks.ilog.Record;
import io.joshworks.ilog.index.KeyComparator;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * -------- HEADER ---------
 * UNCOMPRESSED_SIZE (4bytes)
 * ENTRY_COUNT (4bytes)
 * <p>
 * ------- KEYS REGION -----
 * KEY_ENTRY
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

    private int uncompressedSize;
    private int entries;

    private final List<Key> keys = new ArrayList<>();
    private final ByteBuffer compressedData;

    public HeapBlock(ObjectPool.Pool<? extends Pooled> pool, int blockSize, KeyComparator comparator, boolean direct, Codec codec) {
        super(pool, blockSize, direct);
        this.comparator = comparator;
        this.direct = direct;
        this.codec = codec;
        this.compressedData = Buffers.allocate(blockSize, direct);
    }

    public boolean add(int offset, ByteBuffer record, int bufferPos, int count) {

        //TODO set max uncompressed data size, resize data ByteBuffer

        if (entries == 0) {
            //TODO implement all transition
            state = State.CREATING;
        }

        if (data.remaining() < record.remaining()) {
            return false;
        }

        //uses data as temporary storage
        //TODO check for readonly blocks
        Key k = getOrAllocate(entries);
        k.offset = offset;

        int keySize = Record.KEY.copyTo(record, k.data);
        assert keySize == comparator.keySize();
        k.data.flip();

        int copied = Buffers.copy(record, bufferPos, count, data);
        entries++;
        return true;
    }

    public void from(ByteBuffer block) {
        uncompressedSize = block.getInt();
        entries = block.getInt();

        int keySize = comparator.keySize();

        for (int i = 0; i < entries; i++) {
            Key key = getOrAllocate(i);
            key.offset = data.getInt();
            Buffers.copy(block, block.position(), keySize, key.data);
            Buffers.offsetPosition(block, keySize);
        }

        int copied = Buffers.copy(block, compressedData);
        Buffers.offsetPosition(block, copied);
    }

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

    private Key getOrAllocate(int idx) {
        while (idx >= keys.size()) {
            keys.add(new Key());
        }
        return keys.get(idx);
    }

    public void compress() {
        if (!State.CREATING.equals(state)) {
            throw new IllegalStateException("Cannot compress block: Invalid block state");
        }

        data.flip();
        codec.compress(data, compressedData);
        compressedData.flip();

        state = State.COMPRESSED;
    }

    public void write(Consumer<ByteBuffer> writer) {
        if (!State.COMPRESSED.equals(state)) {
            throw new IllegalStateException("Cannot compress block: Invalid block state");
        }

        data.clear();
        data.putInt(uncompressedSize);
        data.putInt(entries);

        for (int i = 0; i < entries; i++) {
            Key key = keys.get(i);
            data.putInt(key.offset);
            Buffers.copy(key.data, data);
        }

        Buffers.copy(compressedData, data);
        data.flip();

        writer.accept(data);

        state = State.COMPRESSED;
    }

    public void clear() {
        for (int i = 0; i < entries; i++) {
            Key key = keys.get(i);
            key.offset = -1;
            key.data.clear();
        }
        data.clear();
        compressedData.clear();
        uncompressedSize = 0;
        entries = 0;

    }

    public void decompress() {
        if (decompressed()) {
            return;
        }
        data.clear();
        codec.decompress(compressedData, data);
        data.flip();
    }

    public int binarySearch(ByteBuffer key, ByteBuffer dst) {
        int keySize = comparator.keySize();

        //do binary search
        //if found check if decompressed
        //if not then decompress and copy

        throw new UnsupportedOperationException();
    }

    public int get(int idx, ByteBuffer dst) {
        if (!decompressed()) {
            decompress();
        }
        int keySize = comparator.keySize();
        throw new UnsupportedOperationException();
    }


    public boolean decompressed() {
        throw new UnsupportedOperationException("TODO");
    }

    public boolean valid() {
        throw new UnsupportedOperationException("TODO");
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

    private class Key {
        private int offset;
        private ByteBuffer data = Buffers.allocate(comparator.keySize(), direct);
    }

    private enum State {
        EMPTY, CREATING, COMPRESSED, READ_ONLY,
    }
}
