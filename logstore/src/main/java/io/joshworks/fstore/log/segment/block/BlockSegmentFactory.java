package io.joshworks.fstore.log.segment.block;

import io.joshworks.fstore.core.Codec;
import io.joshworks.fstore.core.Serializer;
import io.joshworks.fstore.core.io.Storage;
import io.joshworks.fstore.log.appender.SegmentFactory;
import io.joshworks.fstore.log.record.IDataStream;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.Type;

public class BlockSegmentFactory<T> implements SegmentFactory<T> {

    private final BlockFactory<T> blockFactory;
    private final Codec codec;
    private final int maxBlockSize;

    public BlockSegmentFactory(BlockFactory<T> blockFactory,  Codec codec, int maxBlockSize) {

        this.blockFactory = blockFactory;
        this.codec = codec;
        this.maxBlockSize = maxBlockSize;
    }

    @Override
    public Log<T> createOrOpen(Storage storage, Serializer<T> serializer, IDataStream reader, String magic, Type type) {
        return new BlockSegment<>(storage, serializer, reader, magic, type, blockFactory, codec, maxBlockSize);
    }
}
