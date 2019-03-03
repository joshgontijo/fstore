package io.joshworks.fstore.log.segment.block;

import io.joshworks.fstore.core.Codec;

import java.nio.ByteBuffer;

public class FixedSizeEntryBlock extends BaseBlock {

    private final int entrySize;

    public FixedSizeEntryBlock(int maxSize, int entrySize) {
        super(maxSize);
        if (entrySize <= 0) {
            throw new IllegalArgumentException("maxSize must be greater than zero");
        }
        this.entrySize = entrySize;
    }

    protected FixedSizeEntryBlock(Codec codec, ByteBuffer data) {
        super(codec, data);
        this.readOnly = true;
        this.entrySize = first().limit();
    }

    @Override
    public ByteBuffer pack(Codec codec) {
        if (readOnly()) {
            throw new IllegalStateException("Block is read only");
        }
        readOnly = true;
        int entryCount = buffers.size();
        int totalSize = entryCount * entrySize;

        ByteBuffer withHeader = ByteBuffer.allocate(Integer.BYTES + Integer.BYTES + totalSize);
        withHeader.putInt(entryCount);
        withHeader.putInt(entrySize);
        for (ByteBuffer data : buffers) {
            if (data.limit() != entrySize) {
                throw new IllegalStateException("Invalid entry size, expected " + entrySize + ", got " + data.limit());
            }
            withHeader.put(data);
        }

        withHeader.flip();
        return codec.compress(withHeader);
    }

    @Override
    protected int unpack(Codec codec, ByteBuffer blockData) {
        ByteBuffer decompressed = codec.decompress(blockData);
        int entryCount = decompressed.getInt();
        int entriesSize = decompressed.getInt();
        for (int i = 0; i < entryCount; i++) {
            int dataEnd = decompressed.position() + entriesSize;
            decompressed.limit(dataEnd);
            ByteBuffer bb = decompressed.slice().asReadOnlyBuffer();
            if (bb.limit() != entriesSize) {
                throw new IllegalStateException("Invalid entry size, expected " + entrySize + ", got " + bb.limit());
            }
            buffers.add(bb);
        }
        return entryCount * entriesSize;
    }

    public static BlockFactory factory(int entrySize) {
        return new FixedSizeBlockFactory(entrySize);
    }

    private static class FixedSizeBlockFactory implements BlockFactory {

        private final int entrySize;

        private FixedSizeBlockFactory(int entrySize) {
            this.entrySize = entrySize;
        }

        @Override
        public Block create(int maxBlockSize) {
            return new FixedSizeEntryBlock(maxBlockSize, entrySize);
        }

        @Override
        public Block load(Codec codec, ByteBuffer data) {
            return new FixedSizeEntryBlock(codec, data);
        }
    }

}
