package io.joshworks.fstore.log.segment.block;

import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * A variable length entry block. Format:
 * ---------- HEADER -------------
 * |---- ENTRY_COUNT (4bytes) ----|
 * <p>
 * ---------- BODY -------------
 * |---- ENTRY_1_LEN (4bytes) ---|
 * |----- ENTRY_1 (NBytes) ----|
 * |---- ENTRY_2_LEN (4bytes) ---|
 * |----- ENTRY_2 (NBytes) ----|
 * ...
 * |---- ENTRY_N_LEN (4bytes) ---|
 * |----- ENTRY_N (NBytes) ----|
 */
public class Block implements Iterable<ByteBuffer> {

    protected final ByteBuffer data;
    protected final List<Integer> lengths = new ArrayList<>();
    protected final List<Integer> positions = new ArrayList<>();
    protected final boolean readOnly;

    //returns the uncompressed size
    public Block(int blockSize, boolean direct) {
        if (blockSize <= 0) {
            throw new IllegalArgumentException("maxSize must be greater than zero");
        }
        this.data = createBuffer(blockSize, direct);
        this.data.position(blockHeaderSize());
        this.readOnly = false;
    }

    protected Block(Codec codec, ByteBuffer data, boolean direct) {
        this.readOnly = true;
        this.data = this.unpack(codec, data, direct);
    }

    protected ByteBuffer createBuffer(int maxSize, boolean direct) {
        return direct ? ByteBuffer.allocateDirect(maxSize) : ByteBuffer.allocate(maxSize);
    }

    //returns true if added, false otherwise
    public boolean add(ByteBuffer entry) {
        if (!checkConstraints(entry)) {
            return false;
        }
        int entrySize = entry.remaining();

        lengths.add(entrySize);
        data.putInt(entrySize);
        data.put(entry);
        return true;
    }

    protected boolean checkConstraints(ByteBuffer entry) {
        if (readOnly) {
            throw new IllegalStateException("Block is read only");
        }
        int entrySize = entry.remaining();
        validateEntry(entry);
        return entrySize + entryHeaderSize() <= data.remaining();
    }

    private void validateEntry(ByteBuffer entry) {
        int entrySize = entry.remaining();
        int maxEntrySize = data.capacity() - blockHeaderSize() - entryHeaderSize();
        if (entrySize > maxEntrySize) { //data - entryCount - 1 entrySize
            throw new IllegalArgumentException("Record size (" + entrySize + ") cannot be greater than (" + data.capacity() + ")");
        }
        if (entrySize == 0) {
            throw new IllegalArgumentException("Empty buffer is not allowed");
        }
    }

    public void pack(Codec codec, ByteBuffer dst) {
        if (readOnly()) {
            throw new IllegalStateException("Block is read only");
        }
        if (entryCount() == 0) {
            return;
        }
        int entryCount = entryCount();
        int uncompressedSize = uncompressedSize();

        data.putInt(0, entryCount);
        data.flip();

        dst.putInt(uncompressedSize);
        codec.compress(data, dst);
    }

    public void clear() {
        lengths.clear();
        positions.clear();
        this.data.position(blockHeaderSize());
    }

    protected ByteBuffer unpack(Codec codec, ByteBuffer blockData, boolean direct) {
        int uncompressedSize = blockData.getInt();

        ByteBuffer data = createBuffer(uncompressedSize, direct);
        codec.decompress(blockData, data);
        data.flip();

        int entryCount = data.getInt();
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
        return data.asReadOnlyBuffer().limit(pos + len).position(pos);
    }

    public boolean readOnly() {
        return readOnly;
    }

    public boolean isEmpty() {
        return lengths.isEmpty();
    }

    protected int entryHeaderSize() {
        return Integer.BYTES;
    }

    protected int blockHeaderSize() {
        return Integer.BYTES;
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

    private static final class BlockEntryIterator implements Iterator<ByteBuffer> {

        private final Block block;
        private int idx;

        private BlockEntryIterator(Block block) {
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

    public static BlockFactory vlenBlock() {
        return new VLenBlockFactory(false);
    }

    public static BlockFactory vlenBlock(boolean direct) {
        return new VLenBlockFactory(direct);
    }

    public static BlockFactory flenBlock(int entrySize) {
        return new FixedSizeBlockFactory(false, entrySize);
    }

    public static BlockFactory flenBlock(boolean direct, int entrySize) {
        return new FixedSizeBlockFactory(direct, entrySize);
    }

    private static class FixedSizeBlockFactory implements BlockFactory {

        private final boolean direct;
        private final int entrySize;

        private FixedSizeBlockFactory(boolean direct, int entrySize) {
            this.direct = direct;
            this.entrySize = entrySize;
        }

        @Override
        public Block create(int maxBlockSize) {
            return new FixedSizeEntryBlock(maxBlockSize, direct, entrySize);
        }

        @Override
        public Block load(Codec codec, ByteBuffer data) {
            return new FixedSizeEntryBlock(codec, data, direct);
        }
    }

    private static class VLenBlockFactory implements BlockFactory {

        private final boolean direct;

        private VLenBlockFactory(boolean direct) {
            this.direct = direct;
        }

        public Block create(int maxBlockSize) {
            return new Block(maxBlockSize, direct);
        }

        public Block load(Codec codec, ByteBuffer data) {
            return new Block(codec, data, direct);
        }
    }

}
