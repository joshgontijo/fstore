package io.joshworks.fstore.log.segment.block;

import io.joshworks.fstore.core.Codec;

import java.nio.ByteBuffer;

/**
 * An space optmized block, in which entry size is stored only once, since all entries have the same size. Format:
 * <p>
 * ---------- BLOCK HEADER -------------
 * |---- UNCOMPRESSED_SIZE (4bytes) ----|
 * |---- ENTRY_COUNT (4bytes) ----|
 * |---- ENTRY_SIZE (4bytes) ----|
 * <p>
 * ---------- BODY -------------
 * |----- ENTRY_1 (XBytes) ----|
 * |----- ENTRY_2 (XBytes) ----|
 * ...
 * |----- ENTRY_N (XBytes) ----|
 */
public class FixedSizeEntryBlock extends Block {

    private int entrySize;

    public FixedSizeEntryBlock(int maxSize, int entrySize) {
        super(maxSize);
        this.entrySize = entrySize;
    }

    protected FixedSizeEntryBlock(Codec codec, ByteBuffer data) {
        super(codec, data);
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
    protected void writeBlockHeader(ByteBuffer dst) {
        super.writeBlockHeader(dst);
        dst.putInt(entrySize);
    }

    @Override
    public int blockHeaderSize() {
        return super.blockHeaderSize() + Integer.BYTES;
    }

    @Override
    protected ByteBuffer unpack(Codec codec, ByteBuffer compressed) {

        //header
        int entryCount = compressed.getInt(); //parent
        int uncompressedSize = compressed.getInt(); //parent
        int entrySize = compressed.getInt();

        ByteBuffer data = createBuffer(uncompressedSize);
        codec.decompress(compressed, data);
        data.flip();

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
}
