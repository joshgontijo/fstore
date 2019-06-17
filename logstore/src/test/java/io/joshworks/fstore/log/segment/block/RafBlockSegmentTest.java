package io.joshworks.fstore.log.segment.block;

import io.joshworks.fstore.codec.snappy.SnappyCodec;
import io.joshworks.fstore.core.io.StorageMode;
import io.joshworks.fstore.core.io.buffers.LocalGrowingBufferPool;
import io.joshworks.fstore.core.util.Memory;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.record.DataStream;
import io.joshworks.fstore.log.segment.WriteMode;
import io.joshworks.fstore.serializer.Serializers;

import java.io.File;

public class RafBlockSegmentTest extends BlockSegmentTest {

    private static final int MAX_ENTRY_SIZE = 1024 * 1024 * 5;
    private static final double CHECKSUM_PROB = 1;
    private static final int BUFFER_SIZE = Memory.PAGE_SIZE;

    @Override
    BlockSegment<String> open(File file) {
        return new BlockSegment<>(
                file, StorageMode.RAF,
                Size.MB.of(10),
                new DataStream(new LocalGrowingBufferPool(false), CHECKSUM_PROB, MAX_ENTRY_SIZE, BUFFER_SIZE),
                "magic",
                WriteMode.LOG_HEAD,
                Serializers.STRING,
                VLenBlock.factory(),
                new SnappyCodec(),
                Memory.PAGE_SIZE);
    }
}
