package io.joshworks.fstore.log.segment.block;

import io.joshworks.fstore.codec.snappy.SnappyCodec;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.io.StorageProvider;
import io.joshworks.fstore.core.io.buffers.SingleBufferThreadCachedPool;
import io.joshworks.fstore.core.util.Memory;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.record.DataStream;
import io.joshworks.fstore.log.segment.header.Type;
import io.joshworks.fstore.serializer.Serializers;

import java.io.File;

public class RafBlockSegmentTest extends BlockSegmentTest {

    private static final long MAX_ENTRY_SIZE = 1024 * 1024 * 5L;
    private static final double CHCKSUM_PROB = 1;

    @Override
    BlockSegment<String> open(File file) {
        return new BlockSegment<>(
                StorageProvider.of(StorageMode.RAF).create(file, Size.MB.of(10)),
                new DataStream(new SingleBufferThreadCachedPool(false), CHCKSUM_PROB, MAX_ENTRY_SIZE),
                "magic",
                Type.LOG_HEAD,
                Serializers.STRING,
                VLenBlock.factory(),
                new SnappyCodec(),
                Memory.PAGE_SIZE);
    }
}
