package io.joshworks.fstore.log.segment.block;

import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;

import java.nio.ByteBuffer;

public class BlockSerializer<T> implements Serializer<Block<T>> {

    private final Codec codec;
    private final BlockFactory<T> blockFactory;
    private final Serializer<T> serializer;

    public BlockSerializer(Codec codec, BlockFactory<T> blockFactory, Serializer<T> serializer) {
        this.codec = codec;
        this.blockFactory = blockFactory;
        this.serializer = serializer;
    }

    @Override
    public ByteBuffer toBytes(Block<T> data) {
        return data.pack(codec);
    }

    @Override
    public void writeTo(Block<T> data, ByteBuffer dest) {
        //do nothing
    }

    @Override
    public Block<T> fromBytes(ByteBuffer buffer) {
        return blockFactory.load(serializer, codec, buffer);
    }
}