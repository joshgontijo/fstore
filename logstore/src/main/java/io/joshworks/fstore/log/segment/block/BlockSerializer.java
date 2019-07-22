package io.joshworks.fstore.log.segment.block;

import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;

public class BlockSerializer implements Serializer<Block> {


    private final Codec codec;
    private final BlockFactory blockFactory;

    public BlockSerializer(Codec codec, BlockFactory blockFactory) {
        this.codec = codec;
        this.blockFactory = blockFactory;
    }

    @Override
    public void writeTo(Block block, ByteBuffer dst) {
        block.pack(codec, dst);
    }

    @Override
    public Block fromBytes(ByteBuffer src) {
        return blockFactory.load(codec, src);
    }
}