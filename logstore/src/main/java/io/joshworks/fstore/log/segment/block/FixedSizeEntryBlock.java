package io.joshworks.fstore.log.segment.block;

import io.joshworks.fstore.core.Codec;

import java.nio.ByteBuffer;

/**
 * An space optmized block, in which entry size is stored only once, since all entries have the same size. Format:
 * <p>
 * |---- ENTRY_COUNT (4bytes) ----|
 * |---- ENTRIES_LEN (4bytes) ---|
 * |----- ENTRY_1 (XBytes) ----|
 * |----- ENTRY_2 (XBytes) ----|
 * ...
 * |----- ENTRY_N (XBytes) ----|
 */
public class FixedSizeEntryBlock extends Block {

    private int entrySize;

    public FixedSizeEntryBlock(int maxSize, boolean direct, int entrySize) {
        super(maxSize, direct);
        this.entrySize = entrySize;
    }

    protected FixedSizeEntryBlock(Codec codec, ByteBuffer data, boolean direct) {
        super(codec, data, direct);
    }

    //returns true if added, false otherwise
    @Override
    public boolean add(ByteBuffer entry) {
        if (!checkConstraints(entry)) {
            return false;
        }
        if (entry.remaining() != entrySize) {
            throw new IllegalArgumentException("Expected entry of size: " + entrySize + ", got " + entry.remaining());
        }

        lengths.add(entrySize);
        positions.add(data.position());
        data.put(entry);
        return true;
    }

    @Override
    public void pack(Codec codec, ByteBuffer dst) {
        this.data.putInt(super.blockHeaderSize(), entrySize);
        super.pack(codec, dst);
    }

    @Override
    protected ByteBuffer unpack(Codec codec, ByteBuffer blockData, boolean direct) {
        int uncompressedSize = blockData.getInt();
        ByteBuffer data = createBuffer(uncompressedSize, direct);
        codec.decompress(blockData, data);
        data.flip();

        int entryCount = data.getInt();
        entrySize = data.getInt();
        for (int i = 0; i < entryCount; i++) {
            lengths.add(entrySize);
            positions.add(data.position());
            data.position(data.position() + entrySize);
        }
        if (lengths.size() != entryCount) {
            throw new IllegalStateException("Expected block with " + entryCount + ", got " + lengths.size());
        }

        return data;
    }

    @Override
    public int entryHeaderSize() {
        return 0; //no entry header
    }

    @Override
    public int blockHeaderSize() {
        return super.blockHeaderSize() + Integer.BYTES; //entryCount + entrySize
    }

}
