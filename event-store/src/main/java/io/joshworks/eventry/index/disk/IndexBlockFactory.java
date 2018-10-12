package io.joshworks.eventry.index.disk;

import io.joshworks.eventry.index.IndexEntry;
import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.log.segment.block.Block;
import io.joshworks.fstore.log.segment.block.BlockFactory;

import java.nio.ByteBuffer;

public class IndexBlockFactory implements BlockFactory<IndexEntry> {
    @Override
    public Block<IndexEntry> create(Serializer<IndexEntry> serializer, int maxBlockSize) {
        return new IndexBlock(maxBlockSize);
    }

    @Override
    public Block<IndexEntry> load(Serializer<IndexEntry> serializer, Codec codec, ByteBuffer data) {
        return new IndexBlock(data, codec);
    }
}
