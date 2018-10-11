package io.joshworks.fstore.log;

import io.joshworks.fstore.codec.snappy.SnappyCodec;
import io.joshworks.fstore.core.io.Mode;
import io.joshworks.fstore.core.io.RafStorage;
import io.joshworks.fstore.core.util.Memory;
import io.joshworks.fstore.core.util.Size;
import io.joshworks.fstore.log.record.DataStream;
import io.joshworks.fstore.log.segment.Log;
import io.joshworks.fstore.log.segment.Type;
import io.joshworks.fstore.log.segment.block.BlockSegment;
import io.joshworks.fstore.log.segment.block.VLenBlock;
import io.joshworks.fstore.serializer.Serializers;

import java.io.File;

public class BlockSegmentTest extends SegmentTest {

    @Override
    Log<String> open(File file) {
        return new BlockSegment<>(
                new RafStorage(file, Size.MEGABYTE.toBytes(10), Mode.READ_WRITE),
                Serializers.STRING, new DataStream(), "magic", Type.LOG_HEAD,
                VLenBlock.factory(), new SnappyCodec(), Memory.PAGE_SIZE);
    }
}
