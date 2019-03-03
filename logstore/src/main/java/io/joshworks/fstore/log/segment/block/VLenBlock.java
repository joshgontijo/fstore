package io.joshworks.fstore.log.segment.block;

import io.joshworks.fstore.core.Codec;

import java.nio.ByteBuffer;

public class VLenBlock extends BaseBlock {

    public VLenBlock(int maxSize) {
        super(maxSize);
    }

    protected VLenBlock(Codec codec, ByteBuffer data) {
        super(data.limit());
        this.readOnly = true;
        this.unpack(codec, data);
    }

    public static BlockFactory factory() {
        return new VLenBlockFactory();
    }

    private static class VLenBlockFactory implements BlockFactory {

        @Override
        public VLenBlock create(int maxBlockSize) {
            return new VLenBlock(maxBlockSize);
        }

        @Override
        public Block load(Codec codec, ByteBuffer data) {
            return new VLenBlock(codec, data);
        }
    }

}
