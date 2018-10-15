package io.joshworks.eventry.index.disk;

import io.joshworks.eventry.index.IndexEntry;
import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.log.segment.block.BlockFactory;

import java.nio.ByteBuffer;

public class IndexBlockFactory implements BlockFactory<IndexEntry, IndexBlock> {
    @Override
    public IndexBlock create(Serializer<IndexEntry> serializer, int maxBlockSize) {
        return new IndexBlock(maxBlockSize);
    }

    @Override
    public IndexBlock load(Serializer<IndexEntry> serializer, Codec codec, ByteBuffer data) {
        return new IndexBlock(data, codec);
    }
}
