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
    public ByteBuffer toBytes(Block data) {
        return data.pack(codec);
    }

    @Override
    public void writeTo(Block data, ByteBuffer dst) {
        ByteBuffer packed = data.pack(codec);
        dst.put(packed);
    }

    @Override
    public Block fromBytes(ByteBuffer buffer) {
        return blockFactory.load(codec, buffer);
    }
}