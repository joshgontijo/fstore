package io.joshworks.eventry.index.disk;

import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.log.segment.block.Block;
import io.joshworks.fstore.log.segment.block.BlockFactory;

import java.nio.ByteBuffer;

public class IndexBlockFactory implements BlockFactory {
    @Override
    public Block create(int maxBlockSize) {
        return new IndexBlock(maxBlockSize);
    }

    @Override
    public Block load(Codec codec, ByteBuffer data) {
        return new IndexBlock(codec, data);
    }
}
